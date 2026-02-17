'use client';


import { motion } from 'framer-motion';
import { Crown, Zap, ArrowRight, Target, Radio } from 'lucide-react';
import { cn } from '@/lib/utils';

interface FeedTileProps {
  title: string;
  count: number;
  anchor?: string;
  onClick: () => void;
  index: number;
}

export default function FeedTile({ title, count, anchor, onClick, index }: FeedTileProps) {
  return (
    <motion.div
      layout
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      whileInView={{ opacity: 1, y: 0, transition: { duration: 0.5 } }}
      viewport={{ once: true }}
      transition={{ delay: index * 0.1 }}
      whileHover={{ scale: 1.02 }}
      whileTap={{ scale: 0.98 }}
      onClick={onClick}
      className="neo-card p-5 min-h-[280px] flex flex-col justify-between group cursor-pointer hover:bg-neutral-100 dark:hover:bg-neutral-900 transition-all duration-300 relative overflow-hidden bg-white dark:bg-black border-4 border-black dark:border-white"
    >
        {/* Background Pattern: Tech Dots */}
        <div className="absolute inset-0 opacity-[0.03] pointer-events-none" 
             style={{ backgroundImage: 'radial-gradient(circle, currentColor 1px, transparent 1px)', backgroundSize: '16px 16px' }} 
        />
        
        {/* Giant Background Index */}
        <span className="absolute -top-6 -right-4 text-[120px] font-black leading-none opacity-[0.03] pointer-events-none select-none group-hover:opacity-10 transition-opacity z-0">
            {String(index + 1).padStart(2, '0')}
        </span>

      <div className="relative z-10 flex flex-col h-full pointer-events-none">
        {/* Top Meta Row */}
        <div className="flex items-start justify-between w-full mb-4">
             {/* Source Count Badge */}
             <div className="bg-black text-white dark:bg-white dark:text-black px-3 py-1.5 text-sm font-black uppercase tracking-tight flex items-center gap-2 border-2 border-transparent">
                <Target size={14} strokeWidth={4} />
                {count} {count === 1 ? 'FEEDER' : 'FEEDERS'}
             </div>
             
             {/* Anchor Badge */}
             {anchor && (
                <div className="flex items-center gap-1.5 bg-[#CCFF00]/20 px-3 py-1.5 border-2 border-black/10 group-hover:border-black/20 transition-colors pointer-events-auto relative z-20">
                    <Crown size={14} strokeWidth={3} className="text-black dark:text-white" />
                    <span className="text-xs font-black uppercase truncate max-w-[100px] tracking-tight text-black dark:text-white">{anchor}</span>
                </div>
             )}
        </div>
        
        {/* Static Title (Punchy) */}
        <div className="flex-grow flex flex-col justify-center">
            <span className="text-xs font-black uppercase tracking-widest text-neutral-gray mb-1">
                FEEDING ON
            </span>
            <h2 className="text-5xl md:text-6xl lg:text-7xl font-black uppercase tracking-tighter leading-[0.9] text-black dark:text-white break-all line-clamp-3 w-full">
                {title}
            </h2>
        </div>

        {/* Bottom Status Row */}
        <div className="flex items-end justify-between mt-4">
             {/* Live Pulse - MINIMAL DOT ONLY */}
             <div className="flex items-center gap-3 relative">
                <div className="relative flex items-center justify-center w-4 h-4">
                    <motion.div 
                        animate={count > 0 ? { opacity: [0.5, 1, 0.5] } : { opacity: 1 }}
                        transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
                        className={cn(
                            "w-2 h-2 rounded-full",
                            count > 0 ? "bg-[#CCFF00]" : "bg-red-500"
                        )}
                    />
                </div>
                <span className={cn(
                    "text-xs font-black uppercase tracking-widest leading-none mt-0.5",
                    count > 0 ? "text-neutral-500" : "text-red-500"
                )}>
                    {count > 0 ? "FEEDING NOW" : "NEW FEED"}
                </span>
             </div>
             
             {/* Action Arrow */}
             <div className="opacity-0 group-hover:opacity-100 transition-opacity duration-200">
                  <ArrowRight size={32} strokeWidth={4} className="text-black dark:text-white" />
             </div>
        </div>
      </div>
    </motion.div>
  );
}
