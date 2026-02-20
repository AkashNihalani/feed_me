'use client';

import { ArrowUpRight } from 'lucide-react';
import { FireItem } from './types';
import { cn } from '@/lib/utils';
import Link from 'next/link';

interface FireCardCompactProps {
  item: FireItem;
}

export function FireCardCompact({ item }: FireCardCompactProps) {
  return (
    <Link href={item.postUrl} target="_blank" className="block relative group mb-3">
      <div className="relative bg-[#E5E5E5] border-2 border-black p-3 shadow-[4px_4px_0px_0px_#000] transition-transform active:translate-y-1 active:shadow-none flex justify-between items-center overflow-hidden">
        
        {/* Hazard Stripes Background Subtle (Lime/Transparent) */}
        <div className="absolute inset-0 bg-[repeating-linear-gradient(45deg,transparent,transparent_10px,rgba(204,255,0,0.2)_10px,rgba(204,255,0,0.2)_20px)] pointer-events-none" />

        <div className="relative z-10 flex items-center gap-3">
            <div className="text-3xl">
                {item.percentileTag || '🚀'}
            </div>
            <div className="flex flex-col">
                <span className="font-black uppercase text-xl leading-none tracking-tighter text-black">
                   TOP {item.percentile || '1%'}
                </span>
                <span className="font-bold text-xs uppercase tracking-wider opacity-70 text-black">
                   {item.handle}
                </span>
            </div>
        </div>

        <div className="relative z-10 bg-black text-[#CCFF00] p-1">
            <ArrowUpRight size={20} />
        </div>

      </div>
    </Link>
  );
}
