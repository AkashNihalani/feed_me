'use client';

interface FireOverviewProps {
  total: number;
  spark: number;
  burn: number;
  blaze: number;
}

export function FireOverview({ total, spark, burn, blaze }: FireOverviewProps) {
  return (
    <div className="bg-black p-[3px] mb-8">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-[3px]">

        <div className="relative overflow-hidden bg-[#111] text-[#CCFF00] p-4 h-28 flex flex-col justify-between">
          <div className="font-mono font-bold uppercase text-[11px] tracking-widest border-b border-dashed border-neutral-700 pb-1">TOTAL</div>
          <div className="text-6xl font-black leading-none tracking-tighter">{total}</div>
        </div>

        <div className="relative overflow-hidden bg-[#FFD600] text-black p-4 h-28 flex flex-col justify-between">
          <div className="font-mono font-bold uppercase text-[11px] tracking-widest border-b border-dashed border-black/20 pb-1">SPARK</div>
          <div className="text-6xl font-black leading-none tracking-tighter">{spark}</div>
        </div>

        <div className="relative overflow-hidden bg-[#FF6B00] text-black p-4 h-28 flex flex-col justify-between">
          <div className="absolute inset-0 opacity-10 bg-[repeating-linear-gradient(45deg,#000,#000_2px,transparent_2px,transparent_8px)]" />
          <div className="relative z-10 font-mono font-bold uppercase text-[11px] tracking-widest border-b border-dashed border-black/20 pb-1">BURN</div>
          <div className="relative z-10 text-6xl font-black leading-none tracking-tighter">{burn}</div>
        </div>

        <div className="relative overflow-hidden bg-[#FF003C] text-white p-4 h-28 flex flex-col justify-between">
          <div className="font-mono font-bold uppercase text-[11px] tracking-widest border-b border-dashed border-white/20 pb-1">BLAZE</div>
          <div className="text-6xl font-black leading-none tracking-tighter animate-molten-pulse drop-shadow-[0_0_10px_rgba(100,0,0,0.8)]">
            {blaze}
          </div>
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
