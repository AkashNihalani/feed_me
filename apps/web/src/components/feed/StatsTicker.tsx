'use client';

import { motion } from 'framer-motion';

export default function StatsTicker() {
  const tickerText = "INTELLIGENCE ACTIVE  //  FEEDING DATA  //  CONSUMING CONTENT  //  SYSTEM OPTIMAL  //  TRACKING VECTORS  //  ";
  
  return (
    <div className="w-full bg-lime overflow-hidden border-y-4 border-black dark:border-white py-2 mb-8 relative z-20">
      <div className="whitespace-nowrap flex">
        <motion.div
            className="flex"
            animate={{ x: [0, -1000] }}
            transition={{
                repeat: Infinity,
                ease: "linear",
                duration: 20
            }}
        >
            <span className="text-xl font-black uppercase tracking-tight text-black mr-4">
                {tickerText.repeat(10)}
            </span>
        </motion.div>
      </div>
    </div>
  );
}
