'use client';

import { motion } from 'framer-motion';
import { Crown, Trash2, ExternalLink, Activity, Eye, MessageSquare, ThumbsUp, Target, Zap } from 'lucide-react';
import { cn } from '@/lib/utils';

interface FeederRowProps {
  handle: string;
  isAnchor: boolean;
  profilePicUrl?: string | null;
  metrics: {
    likes: string;
    comments: string;
    views: string;
    postsTracked: string;
  };
  onMakeAnchor: () => void;
  onRemove: () => void;
  index: number;
}

export default function FeederRow({ handle, isAnchor, profilePicUrl, metrics, onMakeAnchor, onRemove, index }: FeederRowProps) {
  return (
    <motion.div
      layout
      initial={false}
      animate={isAnchor ? { scale: [1, 1.05, 1], zIndex: 50, transition: { duration: 0.4, type: "spring", bounce: 0.5 } } : { scale: 1, zIndex: 1 }}
      exit={{ opacity: 0, scale: 0.9 }}
      transition={{ layout: { duration: 0.3, type: "spring" } }}
      className={cn(
        "neo-card p-4 min-h-[180px] flex flex-col justify-between group relative overflow-hidden transition-all duration-300 border-4",
        isAnchor ? "bg-lime border-black shadow-[8px_8px_0px_0px_#000000]" : "bg-white dark:bg-black border-black dark:border-white hover:border-lime hover:shadow-[8px_8px_0px_0px_#CCFF00]"
      )}
    >
        {/* Background Pattern */}
        <div className="absolute inset-0 opacity-[0.03] pointer-events-none" 
             style={{ backgroundImage: 'radial-gradient(circle, currentColor 1px, transparent 1px)', backgroundSize: '16px 16px' }} 
        />

        {/* Anchor Badge */}
        {isAnchor && (
            <div className="absolute top-0 right-0 bg-black text-lime px-3 py-1 text-[10px] font-black uppercase flex items-center gap-1 z-20 border-b-4 border-l-4 border-black">
                <Crown size={12} strokeWidth={3} />
                ANCHOR
            </div>
        )}

      <div className="relative z-10">
        {/* Handle Header */}
        <div className="flex items-start justify-between mb-4">
            <div className="flex items-center gap-3 w-full">
                {/* Avatar Box */}
                <div className={cn(
                    "w-14 h-14 border-4 flex items-center justify-center font-black text-2xl overflow-hidden shrink-0 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]",
                    isAnchor ? "bg-black text-lime border-black" : "bg-black text-white dark:bg-white dark:text-black border-black dark:border-white"
                )}>
                     {profilePicUrl ? (
                        <img src={profilePicUrl} alt={`@${handle}`} className="w-full h-full object-cover" />
                     ) : (
                        handle.substring(0, 2).toUpperCase()
                     )}
                </div>
                
                {/* Handle Name */}
                <div className="overflow-hidden flex-1 min-w-0">
                     <h3 className={cn(
                        "font-black text-3xl uppercase tracking-tighter leading-[0.9] truncate w-full",
                         isAnchor ? "text-black" : "text-black dark:text-white group-hover:text-lime transition-colors"
                     )}>
                        {handle}
                     </h3>
                     <div className="flex items-center gap-2 mt-1">
                        <span className={cn(
                            "text-[10px] font-bold uppercase tracking-widest px-1.5 py-0.5 border-2",
                            isAnchor ? "border-black text-black" : "border-black/20 text-neutral-gray"
                        )}>
                            @{handle}
                        </span>
                     </div>
                </div>
            </div>
        </div>

        {/* Metrics Grid - Brutalist Stamps */}
        <div className="grid grid-cols-4 gap-2 mb-2">
             <MetricStamp label="LIKES" value={metrics.likes} isAnchor={isAnchor} />
             <MetricStamp label="COMMS" value={metrics.comments} isAnchor={isAnchor} />
             <MetricStamp label="VIEWS" value={metrics.views} isAnchor={isAnchor} />
             <MetricStamp label="POSTS" value={metrics.postsTracked} isAnchor={isAnchor} />
        </div>
      </div>

      {/* Action Bar */}
      <div className={cn(
          "relative z-10 flex items-center justify-between mt-auto pt-3 border-t-4",
          isAnchor ? "border-black" : "border-black/10 dark:border-white/10"
      )}>
        {!isAnchor ? (
             <button 
                onClick={onMakeAnchor}
                className="text-xs font-black uppercase hover:text-lime hover:bg-black hover:px-2 -ml-2 py-1 transition-all flex items-center gap-1 group/btn"
            >
                <Crown size={14} strokeWidth={3} className="group-hover/btn:scale-110 transition-transform" />
                MAKE ANCHOR
            </button>
        ) : (
            <span className="text-xs font-black uppercase text-black flex items-center gap-1">
                <Target size={14} strokeWidth={3} />
                TRACKING
            </span>
        )}

        <div className="flex items-center gap-2">
             <button className={cn(
                 "p-1.5 hover:bg-black hover:text-white transition-colors border-2 border-transparent hover:border-black",
                 isAnchor ? "text-black border-black/10" : "text-neutral-gray dark:text-neutral-400"
             )}>
                <ExternalLink size={16} strokeWidth={2.5} />
            </button>
            <button 
                onClick={onRemove} 
                className={cn(
                    "p-1.5 hover:bg-red-500 hover:text-white transition-colors border-2 border-transparent hover:border-black",
                    isAnchor ? "text-red-600 border-red-600/20" : "text-neutral-gray hover:border-black dark:text-neutral-400"
                )}
            >
                <Trash2 size={16} strokeWidth={2.5} />
            </button>
        </div>
      </div>
    </motion.div>
  );
}

function MetricStamp({ label, value, isAnchor }: { label: string, value: string, isAnchor: boolean }) {
    return (
        <div className={cn(
            "flex flex-col items-center justify-center border-2 p-1",
            isAnchor ? "border-black bg-white/20" : "border-black/5 dark:border-white/10 bg-neutral-100 dark:bg-neutral-900 group-hover:border-black"
        )}>
            <span className={cn(
                "text-[9px] font-black uppercase",
                isAnchor ? "text-black/60" : "text-neutral-gray"
            )}>{label}</span>
            <span className={cn(
                "text-sm font-black leading-none mt-0.5",
                isAnchor ? "text-black" : "text-black dark:text-white"
            )}>{value}</span>
        </div>
    )
}
