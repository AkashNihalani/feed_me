'use client';

import { useState } from 'react';
import { ArrowUpRight, Link2, Sheet, Plus, Zap, Download, Loader2, Check } from 'lucide-react';
import { getSupabase } from '@/lib/supabase';
import { cn } from '@/lib/utils';

const HANDLE_CARDS = [
  { handle: '@trysugar', name: 'SUGAR Cosmetics', status: 'Active', pulse: '📈', lastSync: '2h ago' },
  { handle: '@hudabeauty', name: 'Huda Beauty', status: 'Active', pulse: '🙂', lastSync: '4h ago' },
  { handle: '@rarebeauty', name: 'Rare Beauty', status: 'Active', pulse: '👑', lastSync: '6h ago' },
];

const PLATFORMS = [
  { id: 'linkedin', name: 'LinkedIn', rate: 2.25 },
  { id: 'youtube', name: 'YouTube', rate: 2 },
  { id: 'x', name: 'X', rate: 1.5 },
  { id: 'instagram', name: 'Instagram', rate: 1.75 },
];

export default function FeedPage() {
  const [selectedPlatform, setSelectedPlatform] = useState<string | null>('instagram');
  const [url, setUrl] = useState('');
  const [postCount, setPostCount] = useState<number>(25);
  const [isLoading, setIsLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const currentPlatform = PLATFORMS.find((p) => p.id === selectedPlatform);
  const estimatedCost = currentPlatform ? Number((currentPlatform.rate * postCount).toFixed(2)) : 0;

  const handleFeedMe = async () => {
    if (!selectedPlatform || !url) return;
    setIsLoading(true);
    setError(null);
    setSuccess(false);

    const { data: { user: authUser }, error: authError } = await getSupabase().auth.getUser();
    if (authError || !authUser) {
      setError('You must be logged in to scrape.');
      setIsLoading(false);
      return;
    }

    const requestId = `${Date.now()}-${Math.random().toString(36).substring(2, 9)}`;

    try {
      const response = await fetch('/api/scrape', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          platform: selectedPlatform,
          url,
          postCount,
          userId: authUser.id,
          requestId,
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to scrape data');
      }

      const data = await response.json();
      if (data.success) {
        setSuccess(true);
        setTimeout(() => setSuccess(false), 5000);
      } else {
        throw new Error(data.error || 'Scrape failed to start');
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An unexpected error occurred');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="h-[100dvh] w-screen overflow-y-auto pt-6 pb-28 md:pt-24 md:pb-10 bg-background">
      <div className="w-[94%] md:w-[88%] mx-auto space-y-8">
        {/* Header */}
        <div className="flex items-end justify-between">
          <div>
            <h1 className="text-4xl md:text-6xl font-black uppercase tracking-tighter">FEED</h1>
            <p className="text-xs md:text-sm font-bold uppercase text-neutral-gray">Linked sheets · handle tracking</p>
          </div>
          <button className="hidden md:flex items-center gap-2 border-2 border-black px-4 py-2 font-black uppercase">
            <Plus size={16} /> Add handle
          </button>
        </div>

        {/* Connected Sheet */}
        <section className="neo-card p-5">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 border-2 border-black flex items-center justify-center bg-lime">
                <Sheet size={18} />
              </div>
              <div>
                <div className="font-black uppercase">Delivery Issue</div>
                <div className="text-xs font-bold text-neutral-gray">Connected · 3 handles · Last sync 2h ago</div>
              </div>
            </div>
            <button className="border-2 border-black px-3 py-2 font-black uppercase text-xs flex items-center gap-2">
              Open sheet <ArrowUpRight size={12} />
            </button>
          </div>
        </section>

        {/* Handle Cards */}
        <section>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-lg font-black uppercase">Tracked Handles</h2>
            <button className="md:hidden border-2 border-black px-3 py-1 font-black uppercase text-xs">Add</button>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {HANDLE_CARDS.map((h) => (
              <div key={h.handle} className="neo-card p-4">
                <div className="flex items-center justify-between">
                  <div className="font-black uppercase text-sm">{h.handle}</div>
                  <div className="text-lg">{h.pulse}</div>
                </div>
                <div className="text-xs font-bold text-neutral-gray mt-1">{h.name}</div>
                <div className="mt-3 flex items-center justify-between text-xs font-bold">
                  <span className="border-2 border-black px-2 py-1">{h.status}</span>
                  <span className="text-neutral-gray">Last sync {h.lastSync}</span>
                </div>
                <div className="mt-3 flex items-center gap-2">
                  <button className="border-2 border-black px-2 py-1 text-xs font-black uppercase">Open tab</button>
                  <button className="text-xs font-black uppercase text-neutral-gray">Remove</button>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* One‑off export tool (collapsed) */}
        <details className="neo-card p-5">
          <summary className="cursor-pointer font-black uppercase flex items-center gap-2">
            <Zap size={16} /> One‑off export (optional)
          </summary>
          <div className="mt-4 space-y-4">
            <div className="flex flex-wrap gap-2">
              {PLATFORMS.map((p) => (
                <button
                  key={p.id}
                  onClick={() => setSelectedPlatform(p.id)}
                  className={cn(
                    "border-2 border-black px-3 py-2 font-black uppercase text-xs",
                    selectedPlatform === p.id ? "bg-lime" : "bg-white"
                  )}
                >
                  {p.name}
                </button>
              ))}
            </div>

            <div className="flex flex-col md:flex-row gap-3">
              <input
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="Paste social link"
                className="neo-input"
              />
              <input
                type="number"
                value={postCount}
                onChange={(e) => setPostCount(Number(e.target.value))}
                className="neo-input md:w-[160px]"
              />
              <button
                onClick={handleFeedMe}
                disabled={isLoading}
                className="neo-btn md:w-[200px] flex items-center justify-center gap-2"
              >
                {isLoading ? <Loader2 className="animate-spin" size={16} /> : <Download size={16} />}
                Export
              </button>
            </div>

            <div className="text-xs font-bold text-neutral-gray">Estimated cost: ₹{estimatedCost}</div>
            {error && <div className="text-xs font-bold text-red-600">{error}</div>}
            {success && (
              <div className="text-xs font-black uppercase text-green-700 flex items-center gap-2">
                <Check size={12} /> Export started
              </div>
            )}
          </div>
        </details>
      </div>
    </div>
  );
}
