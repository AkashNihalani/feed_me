'use client';

import { useEffect, useState, Suspense } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import { motion, AnimatePresence } from 'framer-motion';
import { ArrowLeft, Plus, Zap, Crown, LayoutGrid, MessageSquare, Eye, Activity, Target, X } from 'lucide-react';
import FeedTile from '@/components/feed/FeedTile';
import FeederRow from '@/components/feed/FeederRow';
import ScanningCard from '@/components/feed/ScanningCard';
import { cn } from '@/lib/utils';

// Mock Data Types
type Metrics = {
  likes: string;
  comments: string;
  views: string;
  postsTracked: string;
};

type Feeder = {
  handle: string;
  isAnchor: boolean;
  profilePicUrl?: string | null;
  followerCount?: number | null;
  verificationStatus?: 'pending' | 'verified' | 'failed';
  metrics: Metrics;
};

type Feed = {
  id: string;
  title: string;
  feeders: Feeder[];
  metrics: Metrics; // Aggregated metrics
};

// Initial Mock Data
const INITIAL_FEEDS: Feed[] = [];

function FeedPageContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  
  // View is determined by URL param 'id'
  const selectedFeedId = searchParams.get('id');
  const view = selectedFeedId ? 'detail' : 'list';

  // Feeds state needs to persist, so we keep it here
  const [feeds, setFeeds] = useState<Feed[]>(INITIAL_FEEDS);
  const [isAddingFeeder, setIsAddingFeeder] = useState(false);
  const [addingFeeder, setAddingFeeder] = useState<string | null>(null);
  
  // Formatting state for inline creation
  const [isCreatingFeed, setIsCreatingFeed] = useState(false);
  const [newFeedName, setNewFeedName] = useState('');
  const [isBusy, setIsBusy] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);

  const activeFeed = feeds.find(f => f.id === selectedFeedId);

  const loadFeeds = async () => {
    const res = await fetch('/api/feed', { method: 'GET' });
    const json = await res.json();
    if (!res.ok) throw new Error(json.error || 'Failed to load feeds');
    setFeeds((json.feeds || []) as Feed[]);
  };

  useEffect(() => {
    loadFeeds().catch((err) => setApiError(err instanceof Error ? err.message : 'Failed to load feeds'));
  }, []);

  const runFeedAction = async (payload: Record<string, unknown>) => {
    setIsBusy(true);
    setApiError(null);
    try {
      const res = await fetch('/api/feed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || 'Action failed');
      setFeeds((json.feeds || []) as Feed[]);
      return true;
    } catch (err) {
      setApiError(err instanceof Error ? err.message : 'Action failed');
      return false;
    } finally {
      setIsBusy(false);
    }
  };

  const handleFeedClick = (id: string) => {
    router.push(`/?id=${id}`);
  };

  const handleBack = () => {
    router.push('/');
  };

  const handleMakeAnchor = async (feedId: string, handle: string) => {
    await runFeedAction({ action: 'make_anchor', feedId: Number(feedId), handle });
  };

  const handleAddFeeder = async (feedId: string, handle: string) => {
      const cleanHandle = (handle || '').trim().replace(/^@+/, '').toLowerCase();
      if (!cleanHandle) return;

      setAddingFeeder(cleanHandle);
      setApiError(null);
      setIsBusy(true);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 30000);

      try {
        const res = await fetch('/api/feed', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ action: 'add_feeder', feedId: Number(feedId), handle: cleanHandle }),
          signal: controller.signal,
        });
        const json = await res.json();
        if (!res.ok) throw new Error(json.error || 'Unable to add feeder');
        setFeeds((json.feeds || []) as Feed[]);
      } catch (err: any) {
        if (err?.name === 'AbortError') {
          setApiError('Feeder confirmation timed out. Please try again.');
        } else {
          setApiError(err instanceof Error ? err.message : 'Unable to add feeder');
        }
      } finally {
        clearTimeout(timeoutId);
        setAddingFeeder(null);
        setIsBusy(false);
      }
  };

  const removeFeeder = async (feedId: string, handle: string) => {
      await runFeedAction({ action: 'remove_feeder', feedId: Number(feedId), handle });
  }

  const handleCreateFeed = async () => {
      if (!newFeedName.trim()) return;
      const ok = await runFeedAction({ action: 'create_feed', title: newFeedName.trim() });
      if (ok) {
        setNewFeedName('');
        setIsCreatingFeed(false);
      }
  };

  return (
    <div className="h-[100dvh] w-full bg-background overflow-y-auto overflow-x-hidden pb-32">
       
       <AnimatePresence mode="wait">
        {view === 'list' ? (
          <motion.div
            key="list"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="px-4 md:px-6 max-w-7xl mx-auto pt-12 md:pt-16"
          >
             {/* Header Section */}
             <div className="mb-12 relative">
                <h1 className="text-[12vw] md:text-[8vw] font-black leading-[0.8] tracking-tighter uppercase text-black dark:text-white mb-4 transition-colors">
                    FEED<br/>COLLECTION
                </h1>
                <div className="flex items-center gap-4">
                    <div className="h-4 w-4 bg-[#CCFF00] animate-pulse rounded-full shadow-[0_0_10px_#CCFF00]" />
                    <span className="font-mono text-xs md:text-sm font-bold uppercase tracking-widest text-neutral-500">
                        {feeds.length} Active Feeds // Systems Nominal
                    </span>
                </div>
                {apiError && (
                    <div className="mt-4 text-xs md:text-sm font-black uppercase text-red-600">{apiError}</div>
                )}
             </div>

             {/* Bento Grid Layout */}
             <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-12 gap-6 pb-20">
                {feeds.map((feed, index) => (
                  <div 
                    key={feed.id}
                    className={cn(
                        "relative group perspective-1000",
                        index % 3 === 0 ? "lg:col-span-8" : "lg:col-span-4" 
                    )}
                  >
                     <FeedTile 
                        title={feed.title}
                        count={feed.feeders.length}
                        anchor={feed.feeders.find(f => f.isAnchor)?.handle}
                        onClick={() => handleFeedClick(feed.id)}
                        index={index}
                     />
                  </div>
                ))}

                {/* Create New Tile (Inline) */}
                <motion.div 
                    layout
                    initial={{ opacity: 0, y: 50 }}
                    whileInView={{ opacity: 1, y: 0 }}
                    viewport={{ once: true }}
                    className={cn(
                        "lg:col-span-4 min-h-[320px] neo-card transition-all duration-300 relative overflow-hidden",
                        isCreatingFeed ? "bg-black text-white border-black" : "bg-white dark:bg-black hover:bg-neutral-50 dark:hover:bg-neutral-900 border-dashed hover:border-solid cursor-pointer"
                    )}
                    onClick={() => !isCreatingFeed && setIsCreatingFeed(true)}
                >
                    <AnimatePresence mode="wait">
                        {!isCreatingFeed ? (
                            <motion.div 
                                key="cta"
                                initial={{ opacity: 0 }}
                                animate={{ opacity: 1 }}
                                exit={{ opacity: 0 }}
                                className="absolute inset-0 flex flex-col items-center justify-center gap-4 group"
                            >
                                <Plus size={48} className="text-neutral-300 group-hover:text-black dark:group-hover:text-white transition-colors" />
                                <span className="font-black uppercase tracking-widest text-neutral-400 group-hover:text-black dark:group-hover:text-white transition-colors">
                                    Init New Feed
                                </span>
                            </motion.div>
                        ) : (
                            <motion.div 
                                key="form"
                                initial={{ opacity: 0, scale: 0.95 }}
                                animate={{ opacity: 1, scale: 1 }}
                                exit={{ opacity: 0, scale: 0.95 }}
                                className="absolute inset-0 p-8 flex flex-col justify-center"
                            >
                                <label className="text-[10px] md:text-xs font-bold uppercase text-[var(--neon-green)] mb-4 tracking-widest">
                                    What are you planning to feed on?
                                </label>
                                <input 
                                    autoFocus
                                    maxLength={15}
                                    value={newFeedName}
                                    onChange={(e) => setNewFeedName(e.target.value.toUpperCase())}
                                    onKeyDown={(e) => {
                                        if (e.key === 'Enter') handleCreateFeed();
                                        if (e.key === 'Escape') {
                                            setIsCreatingFeed(false);
                                            setNewFeedName('');
                                        }
                                    }}
                                    placeholder="FEED NAME..."
                                    className="bg-transparent border-b-4 border-white/20 focus:border-[#CCFF00] text-4xl md:text-5xl font-black uppercase w-full focus:outline-none placeholder:text-white/20 pb-4 mb-8 transition-colors text-white text-base md:text-5xl"
                                    style={{ fontSize: 'max(16px, 3rem)' }} // Prevent zoom on mobile, large on desktop
                                />
                                <div className="flex items-center justify-between">
                                    <span className="text-[10px] font-bold text-white/40 uppercase">
                                        {newFeedName.length} / 15 CHARS
                                    </span>
                                    <div className="flex gap-4">
                                        <button 
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                setIsCreatingFeed(false);
                                                setNewFeedName('');
                                            }}
                                            className="text-xs font-bold uppercase text-white/60 hover:text-white hover:underline"
                                        >
                                            Cancel
                                        </button>
                                        <button 
                                            onClick={(e) => {
                                                e.stopPropagation();
                                                handleCreateFeed();
                                            }}
                                            disabled={!newFeedName.trim() || isBusy}
                                            className="text-xs font-bold uppercase text-[#CCFF00] hover:text-white disabled:opacity-50 disabled:cursor-not-allowed"
                                        >
                                            Confirm
                                        </button>
                                    </div>
                                </div>
                            </motion.div>
                        )}
                    </AnimatePresence>
                </motion.div>
             </div>
          </motion.div>
        ) : (
          <motion.div
            key="details"
            initial={{ opacity: 0, y: 50 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 50 }}
            className="px-4 md:px-6 max-w-7xl mx-auto min-h-screen pt-12 md:pt-16"
          >
             {/* Feed Detail Header */}
             <div className="flex flex-col md:flex-row items-start md:items-end justify-between gap-6 mb-12 border-b-4 border-black dark:border-white pb-6">
                <div>
                     <button 
                        onClick={() => handleBack()}
                        className="flex items-center gap-2 font-black uppercase text-xs tracking-widest hover:text-[var(--neon-green)] transition-colors mb-4 group text-neutral-500"
                     >
                        <ArrowLeft size={16} className="group-hover:-translate-x-1 transition-transform" />
                        Return to Grid
                     </button>
                     <h1 className="text-6xl md:text-8xl font-black uppercase tracking-tighter leading-[0.85] text-stroke-2 transition-colors">
                        {activeFeed?.title}
                     </h1>
                </div>

                <div className="flex items-center gap-3 w-full md:w-auto">
                    {/* Header Controls (if any) - Removed Add Feeder Button to move to Grid */}
                </div>
             </div>

             {/* Dashboard Stats */}
             <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-12">
                 {/* Total Likes */}
                 <div className="neo-card p-4 bg-[var(--neon-green)] text-black flex flex-col justify-between h-32 md:h-40 group hover:scale-[1.02] transition-transform duration-200">
                    <div className="flex justify-between items-start">
                        <span className="text-xs font-black uppercase opacity-60">Total Likes</span>
                        <Zap className="opacity-20 group-hover:opacity-100 transition-opacity stroke-2" />
                    </div>
                    <span className="text-4xl md:text-5xl font-black tracking-tighter">{activeFeed?.metrics.likes}</span>
                 </div>
                 
                 {/* Comments */}
                 <div className="neo-card p-4 bg-white dark:bg-black flex flex-col justify-between h-32 md:h-40 group hover:scale-[1.02] transition-transform duration-200 shadow-[4px_4px_0px_0px_rgba(0,0,0,0.1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,0.1)]">
                    <div className="flex justify-between items-start">
                        <span className="text-xs font-black uppercase text-neutral-500">Comments</span>
                        <MessageSquare className="text-neutral-300 group-hover:text-black dark:group-hover:text-white transition-colors" />
                    </div>
                    <span className="text-4xl md:text-5xl font-black tracking-tighter transition-colors group-hover:text-[var(--neon-green)]">{activeFeed?.metrics.comments}</span>
                 </div>

                 {/* Views */}
                 <div className="neo-card p-4 bg-black text-white dark:bg-white dark:text-black flex flex-col justify-between h-32 md:h-40 group hover:scale-[1.02] transition-transform duration-200">
                    <div className="flex justify-between items-start">
                        <span className="text-xs font-black uppercase opacity-60">Views</span>
                        <Eye className="opacity-20 group-hover:opacity-100 transition-opacity" />
                    </div>
                    <span className="text-4xl md:text-5xl font-black tracking-tighter">{activeFeed?.metrics.views}</span>
                 </div>

                 {/* Posts Tracked */}
                 <div className="neo-card p-4 border-dashed flex flex-col justify-between h-32 md:h-40 group hover:scale-[1.02] transition-transform duration-200">
                    <div className="flex justify-between items-start">
                        <span className="text-xs font-black uppercase text-neutral-500">Tracked</span>
                        <Activity className="text-neutral-300 group-hover:text-black dark:group-hover:text-white transition-colors" />
                    </div>
                    <span className="text-4xl md:text-5xl font-black tracking-tighter">{activeFeed?.metrics.postsTracked}</span>
                 </div>
             </div>

             {/* Feeders Grid */}
             <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 pb-32">


                 {addingFeeder && (
                    <ScanningCard key="scanning" handle={addingFeeder} />
                 )}
                 {activeFeed?.feeders.length === 0 && !addingFeeder ? (
                     <div className="col-span-full flex flex-col items-center justify-center py-20 border-4 border-dashed border-neutral-800/10 dark:border-white/10 opacity-50">
                         <Target size={48} className="mb-4 text-neutral-400" />
                         <span className="font-black uppercase text-xl text-neutral-500">NO TARGETS ACQUIRED</span>
                     </div>
                 ) : (
                    activeFeed?.feeders.map((feeder, i) => (
                        <FeederRow
                            key={feeder.handle}
                            index={i}
                            handle={feeder.handle}
                            isAnchor={feeder.isAnchor}
                            profilePicUrl={feeder.profilePicUrl}
                            metrics={feeder.metrics}
                            onMakeAnchor={() => activeFeed && handleMakeAnchor(activeFeed.id, feeder.handle)}
                            onRemove={() => activeFeed && removeFeeder(activeFeed.id, feeder.handle)}
                        />
                    ))
                 )}


                {/* Add Feeder Tile (Refactored & Moved) */}
                <motion.div 
                    layout
                    className={cn(
                        "neo-card p-4 min-h-[180px] flex flex-col justify-center items-center group cursor-pointer transition-all duration-300 border-4",
                        isAddingFeeder 
                            ? "bg-[var(--neon-green)] border-black cursor-default shadow-[8px_8px_0px_0px_rgba(0,0,0,1)]" 
                            : "bg-neutral-100 dark:bg-neutral-900 border-dashed border-black/20 hover:border-solid hover:border-black hover:bg-[var(--neon-green)] hover:shadow-[4px_4px_0px_0px_#000]"
                    )}
                    onClick={() => !isAddingFeeder && setIsAddingFeeder(true)}
                 >
                    {!isAddingFeeder ? (
                        <div className="flex flex-col items-center gap-2 group-hover:scale-110 transition-transform">
                             <Plus size={48} strokeWidth={4} className="text-neutral-300 group-hover:text-black transition-colors" />
                             <span className="font-black uppercase tracking-widest text-neutral-300 group-hover:text-black transition-colors">
                                ADD FEEDER
                             </span>
                        </div>
                    ) : (
                        <div className="w-full h-full flex flex-col justify-between" onClick={(e) => e.stopPropagation()}>
                             {/* Input Header */}
                             <div className="flex items-start justify-between mb-4">
                                <div className="flex items-center gap-3 w-full">
                                    <div className="w-14 h-14 border-4 border-black flex items-center justify-center bg-black text-[var(--neon-green)] font-black text-2xl shrink-0">
                                        @
                                    </div>
                                    <input 
                                        autoFocus
                                        placeholder="HANDLE"
                                        className="bg-transparent text-3xl font-black uppercase w-full focus:outline-none placeholder:text-black/30 text-black h-14"
                                        onKeyDown={(e) => {
                                            if (e.key === 'Enter') {
                                                handleAddFeeder(activeFeed!.id, e.currentTarget.value);
                                                setIsAddingFeeder(false);
                                            }
                                            if (e.key === 'Escape') setIsAddingFeeder(false);
                                        }}
                                    />
                                </div>
                             </div>
                             
                             {/* Input Footer */}
                             <div className="flex items-center justify-between mt-auto pt-3 border-t-4 border-black/10">
                                <span className="text-[10px] font-black uppercase text-black/60">
                                    PRESS ENTER
                                </span>
                                <button 
                                    onClick={() => setIsAddingFeeder(false)}
                                    className="text-[10px] font-black uppercase text-black hover:text-red-700 hover:underline"
                                >
                                    CANCEL
                                </button>
                             </div>
                        </div>
                    )}
                 </motion.div>
             </div>

          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export default function FeedPage() {
  return (
    <Suspense fallback={<div className="h-screen w-full bg-background" />}>
      <FeedPageContent />
    </Suspense>
  );
}
