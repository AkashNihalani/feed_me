'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Zap, BarChart3, Download, Layers, Instagram, Linkedin, Twitter, Youtube } from 'lucide-react';
import Image from 'next/image';

const FEATURES = [
  {
    id: 'scrape',
    title: "ONE BUTTON.\nALL DATA.",
    subtitle: "Instant extraction. No rate limits. Pure speed.",
    icon: Zap,
    color: "text-lime"
  },
  {
    id: 'export',
    title: "INSTANT\nEXCEL.",
    subtitle: "Clean datasets. Ready for analysis. Seconds, not hours.",
    icon: Download,
    color: "text-cyan-400"
  },
  {
    id: 'analytics',
    title: "DOMINATE\nTHE NICHE.",
    subtitle: "Spy on competitors. Track viral trends. Own the market.",
    icon: BarChart3,
    color: "text-pink"
  }
];

const PRICING = [
  { icon: Linkedin, label: 'LinkedIn', price: '₹2.25', color: 'text-blue-500' },
  { icon: Youtube, label: 'YouTube', price: '₹2.00', color: 'text-red-500' },
  { icon: Instagram, label: 'Instagram', price: '₹1.75', color: 'text-pink-500' },
  { icon: Twitter, label: 'X', price: '₹1.50', color: 'text-white' },
];

export default function BrutalistSlideshow() {
  const [index, setIndex] = useState(0);

  useEffect(() => {
    const timer = setInterval(() => {
      setIndex((prev) => (prev + 1) % FEATURES.length);
    }, 4000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="relative w-full h-full bg-black flex flex-col justify-between overflow-hidden border-r-8 border-black">
      
      {/* Dynamic Background */}
      <div className="absolute inset-0 z-0 bg-[#050505]">
          {/* Animated Grid Floor */}
          <div className="absolute inset-0 opacity-30 bg-[linear-gradient(to_right,#80808012_1px,transparent_1px),linear-gradient(to_bottom,#80808012_1px,transparent_1px)] bg-[size:50px_50px] [transform:perspective(500px)_rotateX(60deg)_scale(2)] origin-bottom animate-pulse"></div>
          {/* Radial Void */}
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_center,transparent_0%,#000000_100%)]"></div>
      </div>

      {/* Main Content Area - Visual Marvel */}
      <div className="relative z-10 flex-1 flex flex-col items-center justify-center p-8 text-center">
        <AnimatePresence mode="wait">
            <motion.div
                key={index}
                initial={{ opacity: 0, scale: 0.8, y: 50, filter: 'blur(10px)' }}
                animate={{ opacity: 1, scale: 1, y: 0, filter: 'blur(0px)' }}
                exit={{ opacity: 0, scale: 1.2, filter: 'blur(10px)' }}
                transition={{ duration: 0.6, ease: [0.22, 1, 0.36, 1] }}
                className="flex flex-col items-center justify-center h-full w-full"
            >
                {/* Massive Background Icon (Visual Anchor) */}
                <div className="absolute inset-0 flex items-center justify-center overflow-hidden pointer-events-none opacity-20">
                    {(() => {
                         const BgIcon = FEATURES[index].icon;
                         return <BgIcon size={800} strokeWidth={0.5} className={`${FEATURES[index].color} opacity-20 blur-sm`} />
                    })()}
                </div>

                {/* Foreground Icon */}
                {(() => {
                    const Icon = FEATURES[index].icon;
                    return (
                        <div className={`mb-12 relative`}>
                             <div className={`absolute inset-0 ${FEATURES[index].color} blur-3xl opacity-50`}></div>
                             <Icon size={120} strokeWidth={1} className={`relative z-10 ${FEATURES[index].color} drop-shadow-[0_0_25px_rgba(255,255,255,0.5)]`} />
                        </div>
                    )
                })()}

                {/* Typography - "Crazy Vibe" */}
                <h1 className="text-[7vw] font-black italic tracking-tighter text-white leading-[0.85] mb-8 mix-blend-screen drop-shadow-2xl">
                    {FEATURES[index].title.split('\n').map((line, i) => (
                        <span key={i} className="block">{line}</span>
                    ))}
                </h1>
                
                <div className="bg-black/80 border-2 border-white/10 backdrop-blur-xl px-8 py-4 transform skew-x-[-10deg]">
                    <p className="text-2xl font-bold text-gray-200 transform skew-x-[10deg] tracking-wide">
                        {FEATURES[index].subtitle}
                    </p>
                </div>
            </motion.div>
        </AnimatePresence>
      </div>

      {/* Pricing Grid Footing - Solid & Clean (No Bleed) */}
      <div className="relative z-20 bg-black border-t-2 border-white/20 p-8">
          <div className="mb-4 flex items-center justify-center gap-4">
              <span className="h-[2px] w-12 bg-white/20"></span>
              <span className="text-xs font-black uppercase tracking-[0.3em] text-gray-500">Flat Rate Per Post</span>
              <span className="h-[2px] w-12 bg-white/20"></span>
          </div>
          <div className="grid grid-cols-2 gap-4 max-w-2xl mx-auto">
              {PRICING.map((p) => (
                  <div key={p.label} className="flex items-center justify-between p-4 bg-white/5 border border-white/10 hover:border-white/40 hover:bg-white/10 transition-all cursor-crosshair group">
                      <div className="flex items-center gap-3">
                          <p.icon size={20} className={`${p.color} transition-transform group-hover:scale-125`} />
                          <span className="text-base font-bold text-gray-400 group-hover:text-white transition-colors">{p.label}</span>
                      </div>
                      <span className="text-lg font-mono font-black text-white">{p.price}</span>
                  </div>
              ))}
          </div>
      </div>
    </div>
  );
}
