'use client';

import { cn } from '@/lib/utils';
import { FireItem } from './types';
import { motion } from 'framer-motion';

interface FireCardProps {
  item: FireItem;
}

export function FireCard({ item }: FireCardProps) {
  const isSpark = item.urgency === 'watch';
  const isBurn = item.urgency === 'today';
  const isBlaze = item.urgency === 'now';

  const handleTap = () => {
    if (item.postUrl) window.open(item.postUrl, '_blank');
  };

  return (
    <motion.article
      layoutId={item.id}
      whileTap={{ scale: 0.985 }}
      onClick={handleTap}
      className={cn(
        'group relative w-full aspect-[4/5] overflow-hidden bg-black border-[4px] cursor-pointer select-none touch-manipulation',
        isSpark && 'border-[#CCFF00] shadow-[0_0_0_1px_#111,0_0_14px_rgba(204,255,0,0.35)]',
        isBurn && 'border-[#FF6B00] fire-burn-trace shadow-[0_0_0_1px_#111,0_0_16px_rgba(255,107,0,0.22)]',
        isBlaze && 'border-[#FF003C] fire-blaze-pulse shadow-[0_0_0_1px_#111,0_0_20px_rgba(255,0,60,0.32)]'
      )}
    >
      <div className="absolute inset-0 z-0 bg-neutral-900">
        {item.thumbnailUrl ? (
          <img
            src={item.thumbnailUrl}
            alt="Signal Media"
            className="w-full h-full object-cover opacity-95"
            loading="lazy"
          />
        ) : (
          <div className="w-full h-full flex items-center justify-center bg-neutral-800">
            <span className="font-black text-5xl text-neutral-700 uppercase tracking-tighter">NO SIGNAL</span>
          </div>
        )}
      </div>

      <div className="absolute inset-0 z-[5] pointer-events-none bg-[linear-gradient(180deg,rgba(0,0,0,0.08)_0%,rgba(0,0,0,0.18)_35%,rgba(0,0,0,0.72)_100%)]" />

      <div className="absolute top-0 left-0 w-full p-2.5 md:p-3.5 z-20 flex items-start justify-between pointer-events-none">
        <div className="flex gap-1.5 md:gap-2 items-center flex-wrap max-w-[88%]">
          <span
            className={cn(
              'px-2.5 md:px-3 py-1.5 border-[2px] border-black text-[11px] md:text-[13px] font-black uppercase tracking-wider shadow-[3px_3px_0px_black]',
              isSpark && 'bg-[#CCFF00] text-black',
              isBurn && 'bg-[#FF6B00] text-black',
              isBlaze && 'bg-[#FF003C] text-white'
            )}
          >
            {isSpark ? 'SPARK' : isBurn ? 'BURN' : 'BLAZE'}
          </span>
          <span className="px-2.5 md:px-3 py-1.5 border-[2px] border-black text-[11px] md:text-[13px] font-black uppercase tracking-wider bg-white text-black shadow-[3px_3px_0px_black]">
            {item.mediaType || 'POST'}
          </span>
          <span className="px-2.5 md:px-3 py-1.5 border-[2px] border-black text-[11px] md:text-[13px] font-black uppercase tracking-wider bg-white text-black shadow-[3px_3px_0px_black]">
            {item.stage}
          </span>
          {item.delta && (
            <span className="px-2.5 md:px-3 py-1.5 border-[2px] border-black text-[11px] md:text-[13px] font-black uppercase tracking-wider bg-black text-white shadow-[3px_3px_0px_black]">
              {item.delta.startsWith('+') ? '↑ ' : item.delta.startsWith('-') ? '↓ ' : ''}{item.delta}
            </span>
          )}
        </div>
        <span className="px-2.5 md:px-3 py-1.5 border-[2px] border-black bg-white text-black text-lg md:text-2xl leading-none shadow-[3px_3px_0px_black]">
          {item.percentileTag || '🔥'}
        </span>
      </div>

      {item.percentile && (
        <div className="absolute right-2 md:right-3 top-[22%] z-15 pointer-events-none">
          <span className="font-black italic text-[48px] md:text-[64px] tracking-tighter text-white/28 drop-shadow-[0_4px_10px_rgba(0,0,0,0.9)]">
            {item.percentile}%
          </span>
        </div>
      )}

      <div className="absolute bottom-0 left-0 w-full z-20 pointer-events-none">
        <div className="mx-2.5 md:mx-3 mb-2 bg-white border-[3px] border-black shadow-[5px_5px_0px_black] p-2.5 md:p-3.5">
          <div className="inline-flex items-center bg-black text-white px-2 md:px-2.5 py-1 text-[11px] md:text-[12px] font-black uppercase tracking-wide mb-2.5 border border-black">
            {item.handle}
          </div>

          <h3 className="text-black text-[26px] md:text-[36px] font-black uppercase leading-[0.9] tracking-tighter">
            {item.title}
          </h3>

          <p className="mt-2.5 text-[12px] md:text-[15px] font-bold uppercase leading-[1.1] tracking-wide text-neutral-800">
            {item.whyNow}
          </p>

          {item.evidence.length > 0 && (
            <div className="mt-3 flex flex-wrap gap-1.5 md:gap-2">
              {item.evidence.slice(0, 3).map((ev, idx) => (
                <span
                  key={`${item.id}-ev-${idx}`}
                  className="px-2.5 md:px-3 py-1.5 border-[2px] border-black bg-[#F3F3F3] text-black text-[11px] md:text-[13px] font-black uppercase tracking-wide shadow-[3px_3px_0px_black]"
                >
                  {ev}
                </span>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="absolute bottom-1.5 right-2.5 z-30 pointer-events-none">
        <span className="font-mono text-[9px] md:text-[10px] font-black uppercase text-white/85">{item.timeAgo}</span>
      </div>

      <style jsx global>{`
        @keyframes burn-travel {
          0% { box-shadow: inset 0 0 0 0 rgba(255, 210, 170, 0.0); }
          50% { box-shadow: inset 0 0 0 2px rgba(255, 210, 170, 0.85); }
          100% { box-shadow: inset 0 0 0 0 rgba(255, 210, 170, 0.0); }
        }
        .fire-burn-trace {
          animation: burn-travel 1.6s linear infinite;
        }
        @keyframes blaze-pulse {
          0%, 100% { box-shadow: 0 0 0 0 rgba(255, 0, 60, 0.2); }
          50% { box-shadow: 0 0 0 4px rgba(255, 0, 60, 0.35); }
        }
        .fire-blaze-pulse {
          animation: blaze-pulse 2s ease-in-out infinite;
        }
      `}</style>
    </motion.article>
  );
}
