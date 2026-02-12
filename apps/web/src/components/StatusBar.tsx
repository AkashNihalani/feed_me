'use client';

import { useEffect, useState } from 'react';
import { createBrowserClient } from '@supabase/ssr';
import { motion, AnimatePresence } from 'framer-motion';
import { Loader2, Check, AlertTriangle, X, Download } from 'lucide-react';

type ScrapeStatus = 'processing' | 'success' | 'failed' | null;

interface ActiveScrape {
    id: number;
    platform: string;
    target_url: string;
    status: ScrapeStatus;
    file_url?: string;
}

export function StatusBar() {
    const [activeScrape, setActiveScrape] = useState<ActiveScrape | null>(null);
    const [isVisible, setIsVisible] = useState(false);
    
    // Polling Logic
    useEffect(() => {
        const supabase = createBrowserClient(
            process.env.NEXT_PUBLIC_SUPABASE_URL!,
            process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
        );

        let intervalId: NodeJS.Timeout;

        const checkStatus = async () => {
            if (!activeScrape) {
                const { data: processing } = await supabase
                    .from('scrapes')
                    .select('*')
                    .eq('status', 'processing')
                    .order('created_at', { ascending: false })
                    .limit(1)
                    .single();

                if (processing) {
                    setActiveScrape(processing);
                    setIsVisible(true);
                }
            } else {
                const { data: current } = await supabase
                    .from('scrapes')
                    .select('*')
                    .eq('id', activeScrape.id)
                    .single();
                 
                 if (current) {
                     if (current.status !== activeScrape.status) {
                         setActiveScrape(current);
                     }
                 }
            }
        };

        checkStatus();
        intervalId = setInterval(checkStatus, 3000);
        return () => clearInterval(intervalId);
    }, [activeScrape]);

    if (!isVisible || !activeScrape) return null;

    const isProcessing = activeScrape.status === 'processing';
    const isSuccess = activeScrape.status === 'success';
    const isFailed = activeScrape.status === 'failed';

    return (
        <AnimatePresence>
            {isVisible && (
                <motion.div
                    layout
                    initial={{ y: 200 }}
                    animate={{ y: 0 }}
                    exit={{ y: 200 }}
                    className={`fixed bottom-0 left-0 w-full h-28 z-50 border-t-[8px] grid grid-cols-12 items-center px-8 font-black uppercase tracking-widest shadow-[0_-10px_50px_rgba(0,0,0,0.8)]
                        ${isProcessing ? 'bg-black border-lime text-lime' : ''}
                        ${isSuccess ? 'bg-lime border-black text-black' : ''}
                        ${isFailed ? 'bg-red-600 border-black text-white' : ''}
                    `}
                >
                    {/* Left: Icon & State */}
                    <div className="col-span-3 flex items-center gap-6">
                        <AnimatePresence mode="wait">
                            {isProcessing && (
                                <motion.div 
                                    key="processing"
                                    initial={{ opacity: 0, x: -20 }}
                                    animate={{ opacity: 1, x: 0 }}
                                    exit={{ opacity: 0, x: 20 }}
                                    className="flex items-center gap-4"
                                >
                                    <div className="relative">
                                        <div className="absolute inset-0 bg-lime blur-xl animate-pulse"></div>
                                        <Loader2 className="relative animate-spin text-lime" size={36} strokeWidth={4} />
                                    </div>
                                    <div className="leading-none">
                                        <span className="block text-3xl tracking-tighter text-white">STARTING FEED...</span>
                                        <span className="text-xs text-lime animate-pulse">DO NOT CLOSE</span>
                                    </div>
                                </motion.div>
                            )}
                            {isSuccess && (
                                <motion.div 
                                    key="success"
                                    initial={{ opacity: 0, scale: 0.8 }}
                                    animate={{ opacity: 1, scale: 1 }}
                                    className="flex items-center gap-4"
                                >
                                    <div className="bg-black text-lime p-2 border-4 border-black shadow-[6px_6px_0px_0px_rgba(0,0,0,1)]">
                                        <Check size={32} strokeWidth={5} />
                                    </div>
                                    <div className="leading-none">
                                        <span className="block text-3xl tracking-tighter">ORDER UP!</span>
                                        <span className="text-xs opacity-70">FRESH DATA SERVED</span>
                                    </div>
                                </motion.div>
                            )}
                            {isFailed && (
                                <motion.div 
                                    key="failed"
                                    initial={{ opacity: 0 }}
                                    animate={{ opacity: 1 }}
                                    className="flex items-center gap-4"
                                >
                                    <AlertTriangle size={36} strokeWidth={4} />
                                    <div className="leading-none">
                                        <span className="block text-3xl tracking-tighter">FEED FAILED</span>
                                        <span className="text-xs opacity-80">TRY AGAIN</span>
                                    </div>
                                </motion.div>
                            )}
                        </AnimatePresence>
                    </div>

                    {/* Center: Message */}
                    <div className="col-span-5 flex justify-center text-center">
                        <AnimatePresence mode="wait">
                            {isProcessing ? (
                                <motion.div
                                    key="proc-msg"
                                    initial={{ opacity: 0, y: 10 }}
                                    animate={{ opacity: 1, y: 0 }}
                                    exit={{ opacity: 0, y: -10 }}
                                    className="flex flex-col items-center"
                                >
                                    <motion.span 
                                        className="text-4xl text-white tracking-tighter truncate max-w-[600px]"
                                        animate={{ opacity: [1, 0.4, 1] }}
                                        transition={{ duration: 1.5, repeat: Infinity }}
                                    >
                                        FEEDING ON {activeScrape.platform}
                                    </motion.span>
                                    <span className="text-xs text-lime opacity-60 font-mono">
                                        {activeScrape.target_url}
                                    </span>
                                </motion.div>
                            ) : (
                                <motion.div
                                    key="done-msg"
                                    initial={{ opacity: 0, scale: 0.9 }}
                                    animate={{ opacity: 1, scale: 1 }}
                                    className="text-5xl font-black italic tracking-tighter"
                                >
                                    {isSuccess ? 'DELIVERY COMPLETE' : 'COULD NOT REACH TARGET'}
                                </motion.div>
                            )}
                        </AnimatePresence>
                    </div>

                    {/* Right: Actions - MEGA BUTTON */}
                    <div className="col-span-4 flex items-center justify-end gap-6">
                        <AnimatePresence>
                            {isSuccess && (
                                <motion.div
                                    initial={{ x: 100, opacity: 0 }}
                                    animate={{ x: 0, opacity: 1 }}
                                    transition={{ type: 'spring', damping: 20, stiffness: 300 }}
                                >
                                    {activeScrape.file_url ? (
                                        <motion.a 
                                            href={activeScrape.file_url}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            whileHover={{ scale: 1.05, x: -4, y: -4, boxShadow: '8px 8px 0px 0px #000000' }}
                                            whileTap={{ scale: 0.95, x: 0, y: 0, boxShadow: '0px 0px 0px 0px #000000' }}
                                            className="flex items-center gap-3 bg-black text-white px-8 py-4 border-4 border-black shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] cursor-pointer no-underline group active:bg-white active:text-black transition-colors"
                                        >
                                            <Download size={32} className="group-hover:animate-bounce" />
                                            <div className="flex flex-col items-start leading-none">
                                                <span className="text-xs text-lime font-bold">READY TO EAT</span>
                                                <span className="text-2xl font-black tracking-tighter">DOWNLOAD DATA</span>
                                            </div>
                                        </motion.a>
                                    ) : (
                                        <div className="flex items-center gap-2 px-8 py-4 border-4 border-black opacity-50 cursor-wait">
                                            <Loader2 className="animate-spin" />
                                            <span>PREPARING MENU...</span>
                                        </div>
                                    )}
                                </motion.div>
                            )}
                        </AnimatePresence>
                        
                        <button 
                            onClick={() => {
                                setIsVisible(false);
                                setActiveScrape(null);
                            }}
                            className={`p-3 transition-transform hover:rotate-90 hover:scale-110 active:scale-90 rounded-full
                                ${isProcessing ? 'text-lime hover:text-white' : 'text-black hover:text-white hover:bg-black'}
                            `}
                        >
                            <X size={36} strokeWidth={4} />
                        </button>
                    </div>
                </motion.div>
            )}
        </AnimatePresence>
    );
}
