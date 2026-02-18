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

type AlertRow = {
  id: number;
  alert_family: string | null;
  alert_urgency: string | null;
  alert_type: string | null;
  title: string | null;
  body: string | null;
  payload: unknown;
  created_at: string;
  status: string | null;
  business_date_ist: string | null;
  checkpoint: string | null;
  percentile_tag: string | null;
  percentile_performance: number | null;
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
    urgency: 'watch',
    color: '#CCFF00',
    handle: '@feed',
    title: 'Spark signal example',
    whyNow: 'This is demo mode while real alerts are loading.',
    action: 'Connect Fire tab to live Supabase alerts.',
    velocityTag: '✅',
    stage: 'D1',
    percentile: '33%',
    evidence: [],
    timeAgo: 'just now',
    createdAt: new Date().toISOString(),
    postUrl: 'https://instagram.com/',
    thumbnailUrl: '',
    businessDateKey: format(new Date(), 'yyyy-MM-dd'),
  },
];

function makeDays(): Date[] {
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

function urgencyFromType(alertType: string): AlertUrgency {
  if (alertType === 'blaze') return 'now';
  if (alertType === 'burn') return 'today';
  return 'watch';
}

function toUrgency(value: string, alertType: string): AlertUrgency {
  if (value === 'now' || value === 'today' || value === 'watch') return value;
  return urgencyFromType(alertType);
}

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {};
}

function asString(value: unknown): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number') return String(value);
  return '';
}


function toMediaProxyUrl(url: string): string {
  const u = (url || '').trim();
  if (!u) return '';
  return `/api/media?url=${encodeURIComponent(u)}`;
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
  const payload = asRecord(row.payload);
  const alertType = asString(row.alert_type || 'spark');
  const family = toFamily(asString(row.alert_family || 'velocity'));
  const urgency = toUrgency(asString(row.alert_urgency || ''), alertType);

  const percentileRaw =
    asString(payload.percentile_performance) ||
    (row.percentile_performance != null ? String(row.percentile_performance) : '');

  const checkpointRaw = asString(payload.checkpoint) || asString(row.checkpoint) || 'd1';
  const checkpoint = checkpointRaw.toUpperCase();

  const businessDateKey =
    asString(payload.business_date_ist) ||
    asString(row.business_date_ist) ||
    format(new Date(row.created_at), 'yyyy-MM-dd');

  const velocityTag =
    asString(payload.percentile_tag) ||
    asString(row.percentile_tag) ||
    (alertType === 'blaze' ? '🚀' : alertType === 'burn' ? '🔥' : '✅');

  return {
    id: String(row.id),
    family,
    urgency,
    color: FAMILY_COLOR[family],
    handle: asString(payload.handle) || '@feed',
    title: row.title || `${alertType.toUpperCase()} signal`,
    whyNow: row.body || asString(payload.why_now) || 'Signal crossed threshold.',
    action: asString(payload.action) || 'Review and execute this move in next cycle.',
    velocityTag,
    stage: checkpoint,
    percentile: percentileRaw ? `${percentileRaw}%` : undefined,
    evidence: [],
    timeAgo: timeAgoText(row.created_at),
    createdAt: row.created_at,
    postUrl: asString(payload.post_url) || 'https://instagram.com',
    thumbnailUrl: toMediaProxyUrl(asString(payload.thumbnail_url) || asString(payload.display_url)),
    businessDateKey,
  };
}

export default function FirePage() {
  const days = useMemo(() => makeDays(), []);
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

        const start = new Date();
        start.setDate(start.getDate() - 7);

        const { data, error } = await supabase
          .from('fire_alerts')
          .select(
            'id,alert_family,alert_urgency,alert_type,title,body,payload,created_at,status,business_date_ist,checkpoint,percentile_tag,percentile_performance'
          )
          .in('status', ['new', 'sent'])
          .gte('created_at', start.toISOString())
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

    void load();
  }, []);

  const dateKey = format(activeDate, 'yyyy-MM-dd');
  const dayItems = useMemo(() => items.filter((i) => i.businessDateKey === dateKey), [items, dateKey]);

  const counts = useMemo(
    () => ({
      total: dayItems.length,
      spark: dayItems.filter((i) => i.urgency === 'watch').length,
      burn: dayItems.filter((i) => i.urgency === 'today').length,
      blaze: dayItems.filter((i) => i.urgency === 'now').length,
    }),
    [dayItems]
  );

  return (
    <div className="h-[100dvh] w-full bg-background pt-8 pb-32 md:pt-24 md:pb-10 overflow-y-auto overflow-x-hidden">
      <div className="w-[94%] md:w-[88%] mx-auto">
        <div className="mb-6 flex flex-col gap-1">
          <h1 className="text-6xl md:text-8xl font-black uppercase tracking-tighter leading-none flex items-center gap-4">
            FIRE
            <span className="text-xl md:text-2xl text-neutral-gray tracking-normal font-bold">// INTELLIGENCE</span>
          </h1>
        </div>

        <div className="mb-10">
          <DayPicker days={days} activeDate={activeDate} onSelect={setActiveDate} />
        </div>

        <FireOverview total={counts.total} spark={counts.spark} burn={counts.burn} blaze={counts.blaze} />

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
