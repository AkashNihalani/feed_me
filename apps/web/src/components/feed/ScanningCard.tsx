'use client';

import { motion } from 'framer-motion';

export default function ScanningCard({ handle }: { handle: string }) {
  return (
    <motion.div
        initial={{ opacity: 0, scale: 0.98 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.98 }}
        layout
        className="neo-card p-5 min-h-[180px] flex flex-col items-center justify-center bg-black text-[#39FF14] relative overflow-hidden group border-4 border-black dark:border-white"
    >
        <div className="relative z-10 text-center flex flex-col items-center justify-center h-full">
             {/* Pure Brutalist - No text, just handle, maybe blinking cursor */}
             <motion.h3 
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                className="text-5xl md:text-6xl font-black uppercase tracking-tighter text-[#CCFF00] break-all line-clamp-2 leading-none"
             >
                {handle}
                <motion.span
                    animate={{ opacity: [0, 1, 0] }}
                    transition={{ duration: 0.8, repeat: Infinity }}
                    className="ml-1"
                >_</motion.span>
             </motion.h3>
        </div>
    </motion.div>
  );
}
