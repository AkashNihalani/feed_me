'use client';

import { motion } from 'framer-motion';
import { ArrowUpRight, Clock } from 'lucide-react';
import { cn } from '@/lib/utils';
import { FireItem } from './types';

interface FireCardProps {
  item: FireItem;
}

export function FireCard({ item }: FireCardProps) {
  // Map Urgency to Tier
  const isSpark = item.urgency === 'watch';
  const isBurn = item.urgency === 'today';
  const isBlaze = item.urgency === 'now';

  // Tier Logic: Theme Colors & Animation Speeds
  const tierColor = isSpark 
    ? 'var(--neutral-gray)' 
    : isBurn 
      ? '#FF6B00' // Vibrant Orange
      : '#FF003C'; // Hot Red

  const tierSecondary = isSpark ? 'var(--color-lime)' : '#000000';

  // Live Wire Border Content
  // We use a CSS background gradient for the hatched pattern.
  // Spark: Static Grey/Lime.
  // Burn: Moving Orange/Black (Slow).
  // Blaze: Moving Red/Black (Fast).

  return (
    <div className={cn(
      "relative group bg-white overflow-hidden flex flex-col h-full",
      // Base Border style (thick)
      "border-4 border-transparent" 
    )}>
      {/* 
        LIVE WIRE BORDER LAYER 
        This absolute div sits behind the content to create the specialized border effect.
      */}
      <div 
        className={cn(
          "absolute inset-0 pointer-events-none z-0",
          isSpark && "bg-[repeating-linear-gradient(45deg,#e5e5e5,#e5e5e5_10px,#CCFF00_10px,#CCFF00_20px)]",
          isBurn && "animate-marquee-slow bg-[repeating-linear-gradient(45deg,#FF6B00,#FF6B00_15px,#000_15px,#000_30px)]",
          isBlaze && "animate-marquee-fast bg-[repeating-linear-gradient(45deg,#FF003C,#FF003C_15px,#000_15px,#000_30px)]"
        )}
        style={{
          backgroundSize: '200% 200%', // Needed for the scrolling animation
        }}
      />
      
      {/* INNER CONTENT CONTAINER (White Background to mask the center of the border div) */}
      <div className="relative z-10 flex flex-col h-full bg-white m-[4px]">
        {/* HEADER */}
        <div 
          className="h-9 flex items-center justify-between px-3 border-b-4 border-black"
          style={{ backgroundColor: tierColor }}
        >
           <div className={cn(
             "text-xs font-black uppercase flex items-center gap-2",
             isSpark ? "text-lime" : "text-white"
           )}>
             <span className="bg-black text-white px-1 py-0.5">#{item.id}</span>
             <span>{item.family}</span>
           </div>
           <div className={cn(
             "text-xs font-bold uppercase flex items-center gap-1", 
             isSpark ? "text-black" : "text-white"
           )}>
             <Clock size={12} strokeWidth={3} /> {item.timeAgo}
           </div>
        </div>

        {/* MEDIA (4:5 Aspect Ratio Enforced) */}
        <div className="relative w-full aspect-[4/5] overflow-hidden border-b-4 border-black bg-neutral-100 group">
          {item.thumbnailUrl ? (
             <img 
               src={item.thumbnailUrl} 
               alt={item.title}
               className="w-full h-full object-cover filter grayscale group-hover:grayscale-0 transition-all duration-500"
             />
          ) : (
            // Fallback Pattern
            <div className="w-full h-full bg-[radial-gradient(circle_at_center,_var(--tw-gradient-stops))] from-gray-200 via-gray-300 to-gray-400 opacity-50" />
          )}
          
          {/* Velocity Stamp (Rotated) */}
          <div className="absolute top-3 right-3 bg-white border-2 border-black p-1.5 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] rotate-[6deg] group-hover:rotate-0 transition-transform duration-300 z-20">
            <div className="text-3xl leading-none">{item.velocityTag}</div>
          </div>
          
          {/* Stage Tag */}
          <div className="absolute bottom-3 left-3 flex gap-1 z-20">
             <span className="text-[10px] font-black uppercase border-2 border-black px-1.5 py-0.5 bg-white text-black shadow-[2px_2px_0px_#000]">{item.stage}</span>
             {item.percentile && (
               <span className="text-[10px] font-black uppercase border-2 border-black px-1.5 py-0.5 bg-lime text-black shadow-[2px_2px_0px_#000]">Top {item.percentile}</span>
             )}
          </div>
        </div>

        {/* BODY */}
        <div className="p-4 flex flex-col grow relative">
          {/* Title */}
          <h3 className={cn(
            "font-black uppercase text-2xl leading-[0.95] mb-4",
            // Heat Haze for Blaze
            isBlaze && "animate-heat-haze text-[#FF003C]"
          )}>
            {item.title}
          </h3>

          {/* Context Blocks */}
          <div className="space-y-4 mb-4">
             <div>
                <span className="block text-[10px] font-black uppercase text-neutral-400 mb-1">Why Now</span>
                <p className="text-xs font-bold uppercase leading-tight">{item.whyNow}</p>
             </div>
             
             <div>
                <span className="block text-[10px] font-black uppercase text-neutral-400 mb-1">The Move</span>
                <p className="text-xs font-bold uppercase leading-tight bg-gray-50 p-2 border-l-4 border-black">
                  {item.action}
                </p>
             </div>
          </div>

          {/* FOOTER ACTION */}
          <div className="mt-auto">
             <a
               href={item.postUrl}
               target="_blank"
               rel="noreferrer"
               className={cn(
                 "w-full flex items-center justify-center gap-2 px-4 py-3 text-sm font-black uppercase transition-all border-2 border-black shadow-[4px_4px_0px_0px_#000] hover:translate-y-[2px] hover:shadow-[2px_2px_0px_0px_#000]",
                 isBlaze ? "bg-[#FF003C] text-white hover:bg-black" : "bg-black text-white hover:bg-lime hover:text-black"
               )}
             >
               Open Post <ArrowUpRight size={16} strokeWidth={3} />
             </a>
          </div>
        </div>
      </div>

      <style jsx global>{`
        @keyframes marquee-c {
          0% { background-position: 0 0; }
          100% { background-position: 50px 50px; }
        }
        .animate-marquee-slow {
          animation: marquee-c 10s linear infinite;
        }
        .animate-marquee-fast {
          animation: marquee-c 5s linear infinite;
        }
        @keyframes heathaze {
          0% { filter: blur(0px); transform: skewX(0deg); }
          25% { filter: blur(0.5px); transform: skewX(-1deg); }
          50% { filter: blur(0px); transform: skewX(0deg); }
          75% { filter: blur(0.5px); transform: skewX(1deg); }
          100% { filter: blur(0px); transform: skewX(0deg); }
        }
        .animate-heat-haze {
          animation: heathaze 2s infinite;
        }
      `}</style>
    </div>
  );
}
