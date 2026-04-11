'use client';

import { motion } from 'framer-motion';

export default function ScanningCard({ handle }: { handle: string }) {
  return (
    <motion.div
        initial={{ opacity: 0, scale: 0.98 }}
        animate={{ opacity: 1, scale: 1 }}
        exit={{ opacity: 0, scale: 0.98 }}
        layout
        className="fm-solid-panel p-5 min-h-[180px] flex flex-col items-center justify-center relative overflow-hidden group"
    >
        <div className="relative z-10 text-center flex flex-col items-center justify-center h-full">
             {/* Pure Brutalist - No text, just handle, maybe blinking cursor */}
             <motion.h3 
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                className="text-5xl md:text-6xl font-black uppercase tracking-tighter text-foreground dark:text-white break-all line-clamp-2 leading-none"
             >
                {handle}
                <motion.span
                    animate={{ opacity: [0, 1, 0] }}
                    transition={{ duration: 0.8, repeat: Infinity }}
                    className="ml-1 inline-block h-[0.9em] w-[0.18em] rounded-full bg-[#E11D48] align-[-0.1em]"
                />
             </motion.h3>
        </div>
    </motion.div>
  );
}
