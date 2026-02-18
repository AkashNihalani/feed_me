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
      
      {/* FIRE BLOCK (Basic) - Grey/Lime */}
      <div className="relative overflow-hidden bg-[#E5E5E5] text-black p-4 border-4 border-black shadow-[4px_4px_0px_0px_#000] h-32 flex flex-col justify-between group">
        <div className="text-lime font-bold uppercase text-[10px] tracking-widest bg-black px-1.5 py-0.5 w-fit">
          FIRE
        </div>
        <div className="text-6xl font-black leading-none tracking-tighter text-black stroke-lime">
          {total} {/* Assuming 'total' is Fire count or we pass specific 'fire' count */}
        </div>
      </div>

      {/* SPARK BLOCK (Low) - Yellow/Black */}
      <div className="relative overflow-hidden bg-[#FFD600] text-black p-4 border-4 border-black shadow-[4px_4px_0px_0px_#000] h-32 flex flex-col justify-between">
         <div className="text-black font-mono text-[10px] font-bold uppercase tracking-widest">
            SPARK
         </div>
         <div className="text-6xl font-black leading-none tracking-tighter text-black">
            {spark}
         </div>
      </div>

      {/* BURN BLOCK (Medium) - Orange/Black Stripes */}
      <div className="relative overflow-hidden bg-[#FF6B00] text-black p-4 border-4 border-black shadow-[4px_4px_0px_0px_#000] h-32 flex flex-col justify-between">
         <div className="absolute inset-0 opacity-10 bg-[repeating-linear-gradient(45deg,#000,#000_2px,transparent_2px,transparent_8px)]" />
         <div className="relative z-10 text-black font-bold uppercase text-[10px] tracking-widest">
            BURN
         </div>
         <div className="relative z-10 text-6xl font-black leading-none tracking-tighter">
            {burn}
         </div>
      </div>

      {/* BLAZE BLOCK (High) - Red/White Molten */}
      <div className="relative overflow-hidden bg-[#FF003C] text-white p-4 border-4 border-black shadow-[4px_4px_0px_0px_#000] h-32 flex flex-col justify-between">
         <div className="text-white font-bold uppercase text-[10px] tracking-widest">
            BLAZE
         </div>
         {/* Molten Core Effect: White text with deep red pulsing glow */}
         <div className="text-6xl font-black leading-none tracking-tighter animate-molten-pulse text-white drop-shadow-[0_0_10px_rgba(100,0,0,0.8)]">
            {blaze}
         </div>
      </div>

      <style jsx global>{`
        @keyframes molten-pulse {
          0%, 100% { text-shadow: 0 0 10px #500000, 0 0 20px #FF003C; }
          50% { text-shadow: 0 0 20px #800000, 0 0 40px #FF003C; }
        }
        .animate-molten-pulse {
          animation: molten-pulse 3s infinite ease-in-out;
        }
      `}</style>
    </div>
  );
}
