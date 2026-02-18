'use client';

import { useEffect, useMemo, useState } from 'react';
import { format } from 'date-fns';
import { AnimatePresence, motion } from 'framer-motion';
import { Loader2 } from 'lucide-react';
import { createBrowserClient } from '@supabase/ssr';
import { DayPicker } from '@/components/ui/DayPicker';
import { FireCard } from '@/components/fire/FireCard';
import { FireOverview } from '@/components/fire/FireOverview';
import { FireItem, AlertFamily, AlertUrgency } from '@/components/fire/types';

// Duplicated from flags page mostly, but cleaned up
type AlertRow = {
  id: number;
  alert_family: string;
  alert_urgency: string;
  alert_type: string;
  title: string | null;
  body: string | null;
  payload: unknown;
  created_at: string;
};

const FAMILY_COLOR: Record<AlertFamily, string> = {
  velocity: '#CCFF00',
  competitive: '#FF2D8A',
  intelligence: '#39A8FF',
};

const MOCK_FIRE: FireItem[] = [
  {
    id: 'demo-1',
    family: 'velocity',
    urgency: 'now', // Blaze
    color: '#CCFF00',
    handle: '@trysugar',
    title: 'Velocity spike on product reel',
    whyNow: 'Crossed threshold in active feed window.',
    action: 'Reply to comments and post follow-up.',
    velocityTag: '🔥',
    stage: 'D3',
    percentile: '08%',
    evidence: [],
    timeAgo: '2h ago',
    createdAt: new Date().toISOString(),
    postUrl: 'https://instagram.com/',
  },
  {
    id: 'demo-2',
    family: 'competitive',
    urgency: 'today', // Burn
    color: '#FF2D8A',
    handle: '@hudabeauty',
    title: 'Competitor format shift detected',
    whyNow: 'Two feeders switched to sidecar-first.',
    action: 'Test one sidecar concept today.',
    velocityTag: '🚀',
    stage: 'D7',
    percentile: '03%',
    evidence: [],
    timeAgo: '4h ago',
    createdAt: new Date().toISOString(),
    postUrl: 'https://instagram.com/',
  },
];

function makeDays(): Date[] {
  // Start from Yesterday (since today is scraping)
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  yesterday.setHours(0, 0, 0, 0);

  // Show last 7 days INCLUDING today (as a disabled state in picker)
  // Logic: 0 = Today (Disabled), 1 = Yesterday (Selected), etc.
  return Array.from({ length: 8 }, (_, i) => {
    const d = new Date();
    d.setDate(d.getDate() - i);
    d.setHours(0, 0, 0, 0);
    return d;
  });
}

function toFamily(value: string): AlertFamily {
  if (value === 'competitive' || value === 'intelligence') return value;
  return 'velocity';
}

function toUrgency(value: string): AlertUrgency {
  if (value === 'now' || value === 'watch') return value;
  return 'today';
}

function timeAgoText(iso: string): string {
  const ts = new Date(iso).getTime();
  const diffMs = Date.now() - ts;
  const hours = Math.floor(diffMs / (1000 * 60 * 60));
  if (hours < 1) return 'just now';
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function mapRowToFire(row: AlertRow): FireItem {
  const payload = (row.payload as any) || {};
  const family = toFamily(row.alert_family);
  const urgency = toUrgency(row.alert_urgency);
  
  const title = row.title || `${family.toUpperCase()} SIGNAL`;
  const whyNow = row.body || payload.why_now || 'Signal crossed threshold.';
  const action = payload.action || 'Execute within window.';

  return {
    id: String(row.id),
    family,
    urgency,
    color: FAMILY_COLOR[family],
    handle: payload.handle || '@feed',
    title,
    whyNow,
    action,
    velocityTag: payload.velocity_tag || (family === 'velocity' ? '🔥' : '👁'),
    stage: payload.stage || 'D3',
    percentile: payload.percentile ? `${payload.percentile}` : undefined,
    evidence: [],
    timeAgo: timeAgoText(row.created_at),
    createdAt: row.created_at,
    postUrl: payload.post_url || 'https://instagram.com',
    thumbnailUrl: payload.thumbnail_url || payload.display_url,
  };
}

export default function FirePage() {
  // Initialize days
  const days = useMemo(() => makeDays(), []);
  
  // Default to index 1 (Yesterday) because index 0 is Today
  const [activeDate, setActiveDate] = useState(days[1]); 
  
  const [items, setItems] = useState<FireItem[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const supabase = createBrowserClient(
           process.env.NEXT_PUBLIC_SUPABASE_URL!,
           process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
        );
        
        // Load last 7 days of alerts
        const start = new Date();
        start.setDate(start.getDate() - 7);
        
        const { data, error } = await supabase
          .from('alert_candidates')
          .select('*')
          .gte('created_at', start.toISOString())
          .in('status', ['candidate', 'selected', 'sent'])
          .order('created_at', { ascending: false })
          .limit(300);

        if (error) throw error;
        
        if (!data || data.length === 0) {
           setItems(MOCK_FIRE);
        } else {
           setItems((data as AlertRow[]).map(mapRowToFire));
        }

      } catch (e) {
        console.error(e);
        setItems(MOCK_FIRE);
      } finally {
        setLoading(false);
      }
    }
    load();
  }, []);

  // Filter items by active date
  const dateKey = format(activeDate, 'yyyy-MM-dd');
  const dayItems = useMemo(() => {
     return items.filter(i => format(new Date(i.createdAt), 'yyyy-MM-dd') === dateKey);
  }, [items, dateKey]);

  // Counts for Overview
  const counts = useMemo(() => {
     return {
       total: dayItems.length,
       spark: dayItems.filter(i => i.urgency === 'watch').length,
       burn: dayItems.filter(i => i.urgency === 'today').length,
       blaze: dayItems.filter(i => i.urgency === 'now').length,
     };
  }, [dayItems]);

  return (
    <div className="min-h-screen w-full bg-background pt-8 pb-32 md:pt-24 md:pb-10 overflow-x-hidden">
      <div className="w-[94%] md:w-[88%] mx-auto">
        
        {/* HEADER */}
        <div className="mb-6 flex flex-col gap-1">
           <h1 className="text-6xl md:text-8xl font-black uppercase tracking-tighter leading-none flex items-center gap-4">
             FIRE
             <span className="text-xl md:text-2xl text-neutral-gray tracking-normal font-bold">
                // INTELLIGENCE
             </span>
           </h1>
        </div>

        {/* TIME TAPE DATE PICKER */}
        <div className="mb-10">
           <DayPicker 
             days={days}
             activeDate={activeDate}
             onSelect={setActiveDate}
           />
        </div>

        {/* OVERVIEW */}
        <FireOverview 
           total={counts.total} 
           spark={counts.spark} 
           burn={counts.burn} 
           blaze={counts.blaze} 
        />

        {/* FEED GRID */}
        {loading ? (
          <div className="h-64 flex items-center justify-center border-4 border-black bg-white">
             <Loader2 className="animate-spin w-8 h-8 mr-2" />
             <span className="font-black uppercase">Loading Intelligence...</span>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
             <AnimatePresence mode="popLayout">
               {dayItems.map((item) => (
                 <motion.div
                   key={item.id}
                   initial={{ opacity: 0, y: 20 }}
                   animate={{ opacity: 1, y: 0 }}
                   exit={{ opacity: 0, scale: 0.95 }}
                   transition={{ duration: 0.3 }}
                 >
                   <FireCard item={item} />
                 </motion.div>
               ))}
             </AnimatePresence>
             
             {dayItems.length === 0 && (
                <div className="col-span-full py-20 text-center border-4 border-dashed border-gray-300">
                   <div className="text-4xl mb-4">🧊</div>
                   <h3 className="text-2xl font-black uppercase text-gray-400">System Cold</h3>
                   <p className="font-bold uppercase text-gray-400">No signals detected for this cycle.</p>
                </div>
             )}
          </div>
        )}

      </div>
    </div>
  );
}
