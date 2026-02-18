'use client';

import { motion } from 'framer-motion';
import { cn } from '@/lib/utils';
import { Activity } from 'lucide-react';

interface FireOverviewProps {
  total: number;
  spark: number;
  burn: number;
  blaze: number;
}

export function FireOverview({ total, spark, burn, blaze }: FireOverviewProps) {
  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
      {/* TOTAL BLOCK */}
      <div className="relative overflow-hidden bg-black text-white p-4 border-4 border-lime shadow-[4px_4px_0px_0px_#CCFF00] h-32 flex flex-col justify-between group">
        <div className="flex justify-between items-start">
           <Activity size={24} className="text-lime animate-pulse" />
           <div className="w-2 h-2 rounded-full bg-lime animate-ping" />
        </div>
        <div className="text-6xl font-black leading-none tracking-tighter">
          {total}
        </div>
      </div>

      {/* SPARK BLOCK */}
      <div className="relative overflow-hidden bg-[#E5E5E5] text-black p-4 border-4 border-black shadow-[4px_4px_0px_0px_#000] h-32 flex flex-col justify-between">
         {/* Technical Barcode Decoration */}
         <div className="absolute top-2 right-2 flex flex-col gap-0.5 opacity-30">
            <div className="w-8 h-1 bg-black" />
            <div className="w-6 h-1 bg-black" />
            <div className="w-8 h-1 bg-black" />
         </div>
         <div className="text-lime font-mono text-[10px] font-bold uppercase tracking-widest">
            ERR_LOW_VOLT
         </div>
         <div className="text-6xl font-black leading-none tracking-tighter text-lime stroke-black drop-shadow-[2px_2px_0px_#000]">
            {spark}
         </div>
      </div>

      {/* BURN BLOCK */}
      <div className="relative overflow-hidden bg-[#FF6B00] text-black p-4 border-4 border-black shadow-[4px_4px_0px_0px_#000] h-32 flex flex-col justify-between">
         <div className="absolute inset-0 opacity-10 bg-[repeating-linear-gradient(45deg,#000,#000_2px,transparent_2px,transparent_8px)]" />
         <div className="relative z-10 text-black font-bold uppercase text-[10px] tracking-widest">
            SYS_HOT
         </div>
         <div className="relative z-10 text-6xl font-black leading-none tracking-tighter">
            {burn}
         </div>
      </div>

      {/* BLAZE BLOCK */}
      <div className="relative overflow-hidden bg-[#FF003C] text-white p-4 border-4 border-black shadow-[4px_4px_0px_0px_#000] h-32 flex flex-col justify-between">
         <div className="absolute top-2 right-2 w-3 h-3 bg-white rounded-full animate-ping" />
         <div className="text-white font-bold uppercase text-[10px] tracking-widest">
            CRITICAL_FAIL
         </div>
         <div className="text-6xl font-black leading-none tracking-tighter animate-heat-haze origin-bottom-left">
            {blaze}
         </div>
      </div>

      <style jsx global>{`
        @keyframes heathaze-text {
          0% { filter: blur(0px); transform: skewX(0deg); }
          50% { filter: blur(1px); transform: skewX(-2deg); }
          100% { filter: blur(0px); transform: skewX(0deg); }
        }
        .animate-heat-haze {
          animation: heathaze-text 0.2s infinite;
        }
      `}</style>
    </div>
  );
}
