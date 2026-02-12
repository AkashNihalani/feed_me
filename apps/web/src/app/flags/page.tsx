'use client';

import { useEffect, useMemo, useState } from 'react';
import { format } from 'date-fns';
import { AnimatePresence, motion } from 'framer-motion';
import { ArrowUpRight, Flag, Clock, Activity, Loader2, Swords } from 'lucide-react';
import { createBrowserClient } from '@supabase/ssr';
import { cn } from '@/lib/utils';
import { DayPicker } from '@/components/ui/DayPicker';

type AlertFamily = 'velocity' | 'competitive' | 'intelligence';
type AlertUrgency = 'now' | 'today' | 'watch';

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

type FlagItem = {
  id: string;
  family: AlertFamily;
  urgency: AlertUrgency;
  color: string;
  handle: string;
  title: string;
  whyNow: string;
  action: string;
  velocityTag: string;
  stage: string;
  percentile?: string;
  evidence: string[];
  timeAgo: string;
  createdAt: string;
  postUrl: string;
  thumbnailUrl?: string;
};

const FAMILY_COLOR: Record<AlertFamily, string> = {
  velocity: '#CCFF00',
  competitive: '#FF2D8A',
  intelligence: '#39A8FF',
};

const MOCK_FLAGS: FlagItem[] = [
  {
    id: 'demo-1',
    family: 'velocity',
    urgency: 'now',
    color: '#CCFF00',
    handle: '@trysugar',
    title: 'Velocity spike on product reel',
    whyNow: 'This post moved into hot band at D3 and stayed above recent baseline.',
    action: 'Reply to comments and post follow-up variation in 12h.',
    velocityTag: '🔥',
    stage: 'D3',
    percentile: '08%',
    evidence: ['+2.1x velocity vs recent video median', 'Top 8% at D3', 'Engagement trend still rising'],
    timeAgo: '2h ago',
    createdAt: new Date().toISOString(),
    postUrl: 'https://instagram.com/',
  },
  {
    id: 'demo-2',
    family: 'competitive',
    urgency: 'today',
    color: '#FF2D8A',
    handle: '@hudabeauty',
    title: 'Competitor format shift detected',
    whyNow: 'Two feeders switched to sidecar-first and their velocity moved up this week.',
    action: 'Test one sidecar concept today with same topic angle.',
    velocityTag: '🚀',
    stage: 'D7',
    percentile: '03%',
    evidence: ['3/5 competitors moved to sidecar', '+1.4x average velocity delta', 'Adoption spike in last 48h'],
    timeAgo: '4h ago',
    createdAt: new Date().toISOString(),
    postUrl: 'https://instagram.com/',
  },
];

const listVariants = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.06, delayChildren: 0.05 },
  },
  exit: { opacity: 0 },
};

const cardVariants = {
  hidden: { opacity: 0, y: 12, scale: 0.98 },
  show: { opacity: 1, y: 0, scale: 1 },
};

function makeDays(): Date[] {
  return Array.from({ length: 7 }, (_, i) => {
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

function asStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map(asString).filter(Boolean);
  if (typeof value === 'string' && value.trim()) return [value.trim()];
  return [];
}

function percentileToDisplay(value: string): string | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  if (trimmed.endsWith('%')) return trimmed;
  const n = Number(trimmed);
  if (Number.isFinite(n)) return `${Math.max(1, Math.min(99, Math.round(n)))}%`;
  return undefined;
}

function generatedPreview(flag: FlagItem): string {
  const bg = flag.color.replace('#', '%23');
  const text = encodeURIComponent(`${flag.handle} ${flag.velocityTag} ${flag.stage}`);
  const title = encodeURIComponent(flag.title.slice(0, 52));
  return `data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='640' height='360'><rect width='100%' height='100%' fill='${bg}'/><rect x='16' y='16' width='608' height='328' fill='black' fill-opacity='0.08' stroke='black' stroke-width='4'/><text x='36' y='78' font-size='28' font-family='Arial Black' fill='black'>${text}</text><text x='36' y='136' font-size='22' font-family='Arial Black' fill='black'>${title}</text></svg>`;
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

function mapRowToFlag(row: AlertRow): FlagItem {
  const payload = asRecord(row.payload);
  const family = toFamily(row.alert_family);
  const urgency = toUrgency(row.alert_urgency);
  const evidence = asStringArray(payload.evidence).slice(0, 3);

  const percentile = percentileToDisplay(
    asString(payload.velocity_percentile) || asString(payload.percentile)
  );

  const createdAt = row.created_at;
  const postUrl = asString(payload.post_url) || asString(payload.url) || 'https://instagram.com/';
  const handle =
    asString(payload.handle) ||
    asString(payload.feeder_handle) ||
    asString(payload.anchor_handle) ||
    '@feed';

  const title = row.title || `${family.toUpperCase()} signal`;
  const whyNow = row.body || asString(payload.why_now) || 'Signal crossed threshold in active feed window.';
  const action = asString(payload.action) || 'Open the post and execute within this content window.';

  return {
    id: String(row.id),
    family,
    urgency,
    color: FAMILY_COLOR[family],
    handle,
    title,
    whyNow,
    action,
    velocityTag: asString(payload.velocity_tag) || (family === 'velocity' ? '🔥' : family === 'competitive' ? '🚀' : '👁'),
    stage: asString(payload.velocity_stage) || asString(payload.stage) || 'D3',
    percentile,
    evidence: evidence.length ? evidence : ['Fresh signal', 'Candidate selected for flags'],
    timeAgo: timeAgoText(createdAt),
    createdAt,
    postUrl,
    thumbnailUrl: asString(payload.thumbnail_url) || asString(payload.display_url) || '',
  };
}

export default function FlagsPage() {
  const days = useMemo(() => makeDays(), []);
  const [activeDate, setActiveDate] = useState(days[0]);
  const [prevIndex, setPrevIndex] = useState(0);
  const [flags, setFlags] = useState<FlagItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [usingMock, setUsingMock] = useState(false);

  useEffect(() => {
    let mounted = true;

    async function loadFlags(): Promise<void> {
      setLoading(true);
      try {
        const supabase = createBrowserClient(
          process.env.NEXT_PUBLIC_SUPABASE_URL!,
          process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
        );

        const start = new Date();
        start.setDate(start.getDate() - 7);

        const { data, error } = await supabase
          .from('alert_candidates')
          .select('id,alert_family,alert_urgency,alert_type,title,body,payload,created_at,status')
          .in('status', ['candidate', 'selected', 'sent'])
          .gte('created_at', start.toISOString())
          .order('created_at', { ascending: false })
          .limit(300);

        if (error) throw error;

        const rows = (data ?? []) as AlertRow[];
        if (!rows.length) {
          if (!mounted) return;
          setFlags(MOCK_FLAGS);
          setUsingMock(true);
          setLoading(false);
          return;
        }

        const mapped = rows.map(mapRowToFlag);
        if (!mounted) return;
        setFlags(mapped);
        setUsingMock(false);
      } catch {
        if (!mounted) return;
        setFlags(MOCK_FLAGS);
        setUsingMock(true);
      } finally {
        if (mounted) setLoading(false);
      }
    }

    void loadFlags();

    return () => {
      mounted = false;
    };
  }, []);

  const activeIndex = days.findIndex((d) => d.toDateString() === activeDate.toDateString());
  const dayKey = format(activeDate, 'yyyy-MM-dd');

  const grouped = useMemo(() => {
    const map = new Map<string, FlagItem[]>();
    for (const day of days) map.set(format(day, 'yyyy-MM-dd'), []);

    for (const item of flags) {
      const key = format(new Date(item.createdAt), 'yyyy-MM-dd');
      if (map.has(key)) map.get(key)!.push(item);
    }
    return map;
  }, [days, flags]);

  const items = grouped.get(dayKey) ?? [];

  const counts = {
    velocity: items.filter((f) => f.family === 'velocity').length,
    competitive: items.filter((f) => f.family === 'competitive').length,
    intelligence: items.filter((f) => f.family === 'intelligence').length,
  };

  const direction = activeIndex >= prevIndex ? 1 : -1;



  return (
    <div className="h-[100dvh] w-screen overflow-y-auto pt-6 pb-28 md:pt-24 md:pb-10 bg-background">
      <div className="w-[94%] md:w-[88%] mx-auto space-y-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-4xl md:text-6xl font-black uppercase tracking-tighter">
            FLAGS
            <span className="text-neutral-gray text-base md:text-lg font-bold ml-3">Feed intelligence alerts</span>
          </h1>
          <p className="text-xs md:text-sm font-bold uppercase text-neutral-gray">
            7-day archive · high signal cards
            {usingMock ? ' · demo mode' : ''}
          </p>
        </div>

        <div className="mb-6">
          <DayPicker 
            days={days} 
            activeDate={activeDate} 
            onSelect={(date) => {
              setPrevIndex(activeIndex < 0 ? 0 : activeIndex);
              setActiveDate(date);
            }} 
          />
        </div>

        {/* Control Panel Overview */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-black text-white p-4 border-2 border-black shadow-[4px_4px_0px_0px_#000] relative overflow-hidden group">
            <div className="absolute top-0 right-0 p-2 opacity-20 group-hover:opacity-30 transition-opacity">
              <Flag size={48} />
            </div>
            <div className="text-[10px] font-black uppercase tracking-widest opacity-60">Total Flags</div>
            <div className="text-5xl md:text-6xl font-black leading-none mt-1 relative z-10">{items.length}</div>
          </div>
          
          <div className="bg-white p-4 border-2 border-black shadow-[4px_4px_0px_0px_var(--color-lime)] relative overflow-hidden group">
            <div className="absolute top-0 right-0 p-2 opacity-10 group-hover:opacity-20 transition-opacity">
              <Activity size={48} />
            </div>
            <div className="text-[10px] font-black uppercase tracking-widest text-neutral-500">Velocity</div>
            <div className="text-4xl md:text-5xl font-black leading-none mt-1 text-lime drop-shadow-[2px_2px_0px_rgba(0,0,0,1)]">
              {counts.velocity}
            </div>
          </div>

          <div className="bg-white p-4 border-2 border-black shadow-[4px_4px_0px_0px_#FF2D8A] relative overflow-hidden group">
            <div className="absolute top-0 right-0 p-2 opacity-10 group-hover:opacity-20 transition-opacity">
              <Swords size={48} />
            </div>
            <div className="text-[10px] font-black uppercase tracking-widest text-neutral-500">Competitive</div>
            <div className="text-4xl md:text-5xl font-black leading-none mt-1 text-[#FF2D8A] drop-shadow-[2px_2px_0px_rgba(0,0,0,1)]">
              {counts.competitive}
            </div>
          </div>

          <div className="bg-white p-4 border-2 border-black shadow-[4px_4px_0px_0px_#39A8FF] relative overflow-hidden group">
            <div className="absolute top-0 right-0 p-2 opacity-10 group-hover:opacity-20 transition-opacity">
              <Clock size={48} />
            </div>
            <div className="text-[10px] font-black uppercase tracking-widest text-neutral-500">Intelligence</div>
            <div className="text-4xl md:text-5xl font-black leading-none mt-1 text-[#39A8FF] drop-shadow-[2px_2px_0px_rgba(0,0,0,1)]">
              {counts.intelligence}
            </div>
          </div>
        </div>

        <div className="relative min-h-[320px]">
          {loading ? (
            <div className="neo-card bg-white p-8 md:col-span-2 flex items-center gap-3">
              <Loader2 className="animate-spin" size={16} />
              <div className="text-sm font-black uppercase">Loading flags…</div>
            </div>
          ) : (
            <AnimatePresence mode="wait">
              <motion.div
                key={dayKey}
                variants={listVariants}
                initial="hidden"
                animate="show"
                exit="exit"
                custom={direction}
                transition={{ duration: 0.2 }}
                className="grid grid-cols-1 md:grid-cols-2 gap-5"
              >
                {items.map((alert, idx) => (
                  <motion.article
                    key={`${alert.id}-${idx}`}
                    variants={cardVariants}
                    className="group border-4 border-black bg-white hover:translate-y-[-4px] transition-transform relative overflow-hidden flex flex-col"
                    style={{ 
                      boxShadow: `8px 8px 0px 0px ${alert.color}` 
                    }}
                  >
                    {/* Header Bar */}
                    <div className="h-9 border-b-4 border-black flex items-center justify-between px-3" style={{ backgroundColor: alert.color }}>
                      <div className="text-xs font-black uppercase text-black flex items-center gap-2">
                        <span className="bg-black text-white px-1 py-0.5">#{alert.id}</span>
                        <span>{alert.family}</span>
                      </div>
                      <div className="text-xs font-bold uppercase flex items-center gap-1">
                        <Clock size={12} strokeWidth={3} /> {alert.timeAgo}
                      </div>
                    </div>

                    <div className="flex flex-col md:grid md:grid-cols-[140px,1fr] h-full">
                      {/* Image / Evidence Column */}
                      <div className="relative border-b-4 md:border-b-0 md:border-r-4 border-black bg-neutral-100 h-48 md:h-full md:min-h-[160px] shrink-0">
                        <img
                          src={alert.thumbnailUrl || generatedPreview(alert)}
                          alt={alert.title}
                          className="absolute inset-0 w-full h-full object-cover filter grayscale group-hover:grayscale-0 transition-all duration-300 contrast-125"
                        />
                        {/* Velocity Stamp */}
                        <div className="absolute top-2 left-2 bg-white border-2 border-black p-1 shadow-[2px_2px_0px_0px_#000] rotate-[-6deg] group-hover:rotate-0 transition-transform z-10">
                          <div className="text-2xl leading-none">{alert.velocityTag}</div>
                        </div>
                      </div>

                      {/* Content Column */}
                      <div className="p-4 flex flex-col h-full bg-white relative grow">
                        {/* Handle & Stage */}
                        <div className="flex items-center justify-between mb-3">
                          <div className="text-xs font-black uppercase tracking-wider text-neutral-500">{alert.handle}</div>
                          <div className="flex gap-1">
                             <span className="text-[10px] font-black uppercase border-2 border-black px-1.5 py-0.5 bg-neutral-200">{alert.stage}</span>
                             {alert.percentile && <span className="text-[10px] font-black uppercase border-2 border-black px-1.5 py-0.5 bg-lime text-black">{alert.percentile}</span>}
                          </div>
                        </div>

                        {/* Title & Body */}
                        <div className="font-black uppercase text-xl md:text-lg leading-[0.9] md:leading-tight mb-3">
                          {alert.title}
                        </div>
                        <div className="text-xs md:text-sm font-bold text-neutral-500 uppercase mb-5 leading-normal">
                          {alert.whyNow}
                        </div>

                        {/* Action / Footer */}
                        <div className="mt-auto pt-4 border-t-2 border-dashed border-black">
                           <div className="flex items-center justify-between gap-3">
                              <div className="flex flex-col flex-1 min-w-0">
                                <span className="text-[10px] font-black uppercase text-neutral-400">The Move</span>
                                <span className="text-xs font-bold uppercase leading-tight line-clamp-2">{alert.action}</span>
                              </div>
                              
                              <a
                                href={alert.postUrl}
                                target="_blank"
                                rel="noreferrer"
                                className="shrink-0 bg-black text-white px-4 py-2 text-xs font-black uppercase flex items-center gap-1 hover:bg-lime hover:text-black transition-colors border-2 border-transparent hover:border-black shadow-[2px_2px_0px_0px_rgba(255,255,255,0.2)]"
                              >
                                Open Post <ArrowUpRight size={14} strokeWidth={3} />
                              </a>
                           </div>
                        </div>
                      </div>
                    </div>
                  </motion.article>
                ))}
                {items.length === 0 && (
                  <motion.div variants={cardVariants} className="border-4 border-black border-dashed p-8 md:col-span-2 flex flex-col items-center justify-center text-center opacity-50">
                    <div className="text-4xl mb-2">📂</div>
                    <div className="text-xl font-black uppercase">No alerts today</div>
                    <div className="text-sm font-bold uppercase mt-1">
                      Check back later or swipe to see past days.
                    </div>
                  </motion.div>
                )}
              </motion.div>
            </AnimatePresence>
          )}
        </div>

        <div className="text-xs font-bold uppercase text-neutral-gray border-t-2 border-black pt-3 flex items-center gap-2">
          <Flag size={12} className="text-lime" /> Alerts roll off after 7 days
        </div>
      </div>
    </div>
  );
}
