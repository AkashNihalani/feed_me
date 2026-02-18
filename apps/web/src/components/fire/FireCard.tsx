'use client';

import { cn } from '@/lib/utils';
import { FireItem } from './types';
import Link from 'next/link';
import { Video, Image as ImageIcon } from 'lucide-react';

interface FireCardProps {
  item: FireItem;
}

export function FireCard({ item }: FireCardProps) {
  const isSpark = item.urgency === 'watch'; // Low
  const isBurn = item.urgency === 'today';  // Medium
  const isBlaze = item.urgency === 'now';   // High

  // Determine Media Icon
  const isVideo = item.postUrl?.includes('reel') || item.postUrl?.includes('tiktok') || item.postUrl?.includes('youtube');
  const MediaIcon = isVideo ? Video : ImageIcon;

  return (
    <div className="relative w-full max-w-md mx-auto mb-12 group perspective-1000">
      
      {/* 1. ANIMATED LIVE WIRE BORDER */}
      <div 
        className={cn(
          "absolute -inset-1 z-0",
          // Spark = Yellow/Black
          isSpark && "bg-[repeating-linear-gradient(45deg,#FFD600,#FFD600_10px,#000_10px,#000_20px)]",
          // Burn = Orange/Black
          isBurn && "animate-marquee-slow bg-[repeating-linear-gradient(45deg,#FF6B00,#FF6B00_15px,#000_15px,#000_30px)]",
          // Blaze = Red/Black
          isBlaze && "animate-marquee-fast bg-[repeating-linear-gradient(45deg,#FF003C,#FF003C_15px,#000_15px,#000_30px)]"
        )}
        style={{
          backgroundSize: '200% 200%',
        }}
      />

      {/* 2. CARD CONTENT CONTAINER */}
      <div className="relative z-10 bg-white border-2 border-black p-3 min-h-[450px] flex flex-col justify-between shadow-[8px_8px_0px_0px_rgba(0,0,0,1)] transition-transform group-hover:-translate-y-1 group-hover:translate-x-1">
        
        {/* HEADER: TIER NAME + EMOJI + TIME */}
        <div className="flex justify-between items-center mb-3 border-b-2 border-black pb-2">
            <div className="flex items-center gap-2">
                <div className={cn(
                    "px-2 py-0.5 text-xs font-black uppercase tracking-widest text-black",
                    isSpark && "bg-[#FFD600]", // Yellow
                    isBurn && "bg-[#FF6B00]", // Orange
                    isBlaze && "bg-[#FF003C] text-white" // Red
                )}>
                    {isSpark ? 'SPARK' : isBurn ? 'BURN' : 'BLAZE'}
                </div>
                {/* Emoji Tag from Item */}
                <div className="text-xl leading-none">
                    {item.velocityTag || '🔥'}
                </div>
            </div>
            <div className="font-mono text-[10px] font-bold opacity-50 uppercase">
                {item.timeAgo}
            </div>
        </div>

        {/* MEDIA: 4:5 Aspect Ratio Container */}
        <div className="relative w-full aspect-[4/5] bg-black border-2 border-black mb-3 overflow-hidden group-hover:shadow-[4px_4px_0px_0px_#CCFF00] transition-shadow">
            
            {/* Top Right: Media Type Icon */}
            <div className="absolute top-2 right-2 z-20 bg-black text-white p-1.5 border border-white/20 shadow-sm">
                <MediaIcon size={16} />
            </div>

            {/* Bottom Left: Massive Overlay Tags */}
            <div className="absolute bottom-2 left-2 z-20 flex flex-col gap-1 items-start">
                 <div className="bg-white border-2 border-black px-2 py-0.5 shadow-[2px_2px_0px_black]">
                    <span className="font-black text-2xl leading-none uppercase">
                        {item.stage}
                    </span>
                 </div>
                 {item.percentile && (
                     <div className="bg-[#CCFF00] border-2 border-black px-2 py-0.5 shadow-[2px_2px_0px_black]">
                        <span className="font-black text-2xl leading-none uppercase">
                            TOP {item.percentile}
                        </span>
                     </div>
                 )}
            </div>

            {/* Media */}
            <img 
               src={item.thumbnailUrl || '/placeholder.jpg'} 
               alt="Alert Media"
               className="w-full h-full object-cover transition-transform duration-700 group-hover:scale-110"
            />
        </div>

        {/* FOOTER */}
        <div>
           <div className="text-xs font-bold uppercase mb-1 text-neutral-400">
             {item.handle}
           </div>
           <h3 className={cn(
             "font-black uppercase text-xl leading-[0.95] mb-2",
             isBlaze && "animate-heat-haze text-[#FF003C]"
           )}>
             {item.title}
           </h3>
           
           <div className="bg-neutral-100 border-l-4 border-black p-2 mb-2">
              <p className="font-mono text-[10px] font-bold uppercase mb-0.5 opacity-60">Insight</p>
              <p className="font-bold text-xs leading-tight uppercase">{item.whyNow}</p>
           </div>

           <Link href={item.postUrl} target="_blank" className="block w-full bg-black text-white py-2.5 text-center font-black uppercase tracking-widest hover:bg-[#CCFF00] hover:text-black transition-colors border-2 border-black shadow-[4px_4px_0px_black] active:shadow-none active:translate-y-1">
              OPEN SIGNAL
           </Link>
        </div>

      </div>
      
      {/* 3. PARTICLES (Blaze Only) */}
      {isBlaze && (
        <div className="absolute inset-x-0 bottom-0 h-full overflow-hidden pointer-events-none z-20 mix-blend-screen">
           <div className="w-1 h-1 bg-yellow-400 absolute bottom-0 left-[20%] animate-[rise_2s_infinite]" />
           <div className="w-1 h-1 bg-red-500 absolute bottom-0 left-[50%] animate-[rise_3s_infinite_0.5s]" />
           <div className="w-2 h-2 bg-orange-500 absolute bottom-0 left-[80%] animate-[rise_2.5s_infinite_1s]" />
           <div className="w-1 h-1 bg-white absolute bottom-0 left-[40%] animate-[rise_1.5s_infinite_0.2s]" />
        </div>
      )}

    </div>
  );
}
