'use client';

import { useEffect, useMemo, useState } from 'react';
import { format, subDays } from 'date-fns';
import { Loader2 } from 'lucide-react';
import { createBrowserClient } from '@supabase/ssr';
import { FireCard } from '@/components/fire/FireCard';
import { FireOverview } from '@/components/fire/FireOverview';
import { DayPicker } from '@/components/ui/DayPicker';
import StatsTicker, { TickerEntry } from '@/components/feed/StatsTicker';
import { FireItem, AlertUrgency } from '@/components/fire/types';

type AlertRow = {
  id: number;
  alert_type: string | null;
  signal_code: string | null;
  title: string | null;
  body: string | null;
  payload: unknown;
  created_at: string;
  status: string | null;
  business_date_ist: string | null;
  checkpoint: string | null;
  post_key: string | null;
};

function asRecord(value: unknown): Record<string, unknown> {
  if (value && typeof value === 'object' && !Array.isArray(value)) return value as Record<string, unknown>;
  return {};
}

function asString(value: unknown): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number') return String(value);
  return '';
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string') {
    const n = Number(value.replace('%', '').trim());
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function cleanCopy(text: string): string {
  return text
    .replace(/\?/g, '')
    .replace(/\s+/g, ' ')
    .replace(/\s+([,.;])/g, '$1')
    .trim();
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

function percentileToTag(percentile: number | null): string {
  if (percentile == null) return '😴';
  if (percentile <= 5) return '🚀';
  if (percentile <= 20) return '🔥';
  if (percentile <= 35) return '✅';
  return '😴';
}

function toUrgency(alertType: string): AlertUrgency {
  if (alertType === 'blaze') return 'now';
  if (alertType === 'burn') return 'today';
  return 'watch';
}

function toIstDateKey(isoLike: string): string {
  const d = new Date(isoLike);
  if (Number.isNaN(d.getTime())) {
    return new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit' }).format(new Date());
  }
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(d);
}

function formatDelta(n: number | null, checkpoint: string): string | undefined {
  if (checkpoint.toLowerCase() === 'd1') return undefined;
  if (n == null || !Number.isFinite(n)) return undefined;
  const rounded = Math.round(n);
  const abs = Math.abs(rounded).toString().padStart(2, '0');
  if (rounded > 0) return `+${abs}`;
  if (rounded < 0) return `-${abs}`;
  return '=00';
}

function compactNumber(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return '';
  if (Math.abs(v) >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (Math.abs(v) >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return String(Math.round(v));
}

function normalizeMediaLabel(raw: string): string {
  const s = raw.trim().toLowerCase();
  if (!s) return 'POST';
  if (s.includes('reel')) return 'REEL';
  if (s.includes('carousel')) return 'CAROUSEL';
  if (s.includes('image') || s.includes('photo')) return 'IMAGE';
  return s.toUpperCase();
}

function inferMediaFromUrl(url: string): string {
  const u = (url || '').toLowerCase();
  if (u.includes('/reel/')) return 'REEL';
  if (u.includes('/tv/')) return 'VIDEO';
  if (u.includes('/p/')) return 'POST';
  return 'POST';
}

function uniqueNonRepeating(parts: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  for (const part of parts) {
    const p = cleanCopy(part);
    if (!p) continue;
    const key = p.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(p);
    if (out.length >= 3) break;
  }
  return out;
}

function metricFromPayload(signalCode: string, payload: Record<string, unknown>): { name: string; value: number | null; label: string } {
  const explicitKey = asString(payload.metric_key || payload.metric_name || payload.primary_metric || payload.reach_metric).toLowerCase();
  const metricValue = asNumber(payload.metric_value);
  const comments = asNumber(payload.comments);
  const views = asNumber(payload.views);
  const likes = asNumber(payload.likes);
  const mediaType = asString(payload.media_type || payload.post_type || payload.media).toLowerCase();

  const resolveByKey = (key: string) => {
    if (key.includes('comment')) return { name: 'comments', value: comments ?? metricValue, label: 'comments' };
    if (key.includes('view') || key.includes('reach')) return { name: 'views', value: views ?? metricValue, label: 'views' };
    if (key.includes('like')) return { name: 'likes', value: likes ?? metricValue, label: 'likes' };
    return null;
  };

  if (explicitKey) {
    const found = resolveByKey(explicitKey);
    if (found) return found;
  }

  if (signalCode === 'A4_engagement_efficiency') {
    return { name: 'comments', value: comments ?? metricValue, label: 'comments' };
  }

  if (metricValue != null) {
    if (views != null && views === metricValue) return { name: 'views', value: views, label: 'views' };
    if (comments != null && comments === metricValue) return { name: 'comments', value: comments, label: 'comments' };
    if (likes != null && likes === metricValue) return { name: 'likes', value: likes, label: 'likes' };
    if (mediaType.includes('reel') && views != null) return { name: 'views', value: views, label: 'views' };
    if (likes != null && comments != null && Math.round(likes + comments) === Math.round(metricValue)) {
      return { name: 'interactions', value: likes + comments, label: 'interactions' };
    }
    return { name: 'metric', value: metricValue, label: 'interactions' };
  }

  if (mediaType.includes('reel') && views != null) return { name: 'views', value: views, label: 'views' };
  if (comments != null) return { name: 'comments', value: comments, label: 'comments' };
  if (views != null) return { name: 'views', value: views, label: 'views' };
  if (likes != null) return { name: 'likes', value: likes, label: 'likes' };

  return { name: 'metric', value: null, label: 'metric' };
}

function buildEvidenceChips(
  payload: Record<string, unknown>,
  signalCode: string,
  topBadgeTags: string[],
  renderedText: string
): string[] {
  const percentile = asNumber(payload.percentile ?? payload.percentile_performance);
  const multiple = asNumber(payload.reach_multiple);
  const daysSince = asNumber(payload.days_since_ceiling);
  const rankRecent = asNumber(payload.rank_in_recent_20);
  const allTimeRank = asNumber(payload.all_time_rank);
  const gap = asNumber(payload.gap ?? payload.gap_vs_anchor);
  const cPct = asNumber(payload.comments_percentile);
  const rPct = asNumber(payload.views_percentile ?? payload.likes_percentile);
  const delta = asNumber(payload.delta);
  const streakCount = asNumber(payload.streak_count);
  const metricValue = asNumber(payload.metric_value ?? payload.views ?? payload.likes);
  const d1_pct = asNumber(payload.d1_percentile ?? payload.d1);
  const curr_pct = asNumber(payload.current_percentile ?? payload.percentile);

  const chips: string[] = [];

  switch (signalCode) {
    case 'A1_baseline_displacement':
      if (multiple) chips.push(`${multiple.toFixed(1)}x BASELINE`);
      if (allTimeRank) chips.push(`#${Math.round(allTimeRank)} ALL-TIME`);
      break;
    case 'A2_ceiling_break':
      if (allTimeRank) chips.push(`#${Math.round(allTimeRank)} ALL-TIME`);
      if (delta) chips.push(`+${Math.round(delta)} SINCE D1`);
      break;
    case 'A3_reach_explosion':
      if (multiple) chips.push(`${multiple.toFixed(1)}x REACH`);
      if (rankRecent) chips.push(`#${Math.round(rankRecent)} RECENT`);
      break;
    case 'A4_engagement_efficiency':
      if (gap) chips.push(`${Math.round(gap)}PT GAP`);
      if (cPct && rPct) chips.push(`C${Math.round(cPct)} VS R${Math.round(rPct)}`);
      break;
    case 'A5_cold_open':
      if (percentile) chips.push(`TOP ${Math.round(percentile)}%`);
      if (metricValue) chips.push(compactNumber(metricValue));
      break;
    case 'A6_consistency_streak':
      if (streakCount) chips.push(`${Math.round(streakCount)} IN STREAK`);
      if (delta) chips.push(`+${Math.round(delta)} SINCE D1`);
      break;
    case 'B1_structural_acceleration':
      if (metricValue) chips.push(compactNumber(metricValue));
      if (rankRecent) chips.push(`#${Math.round(rankRecent)} RECENT`);
      break;
    case 'B2_late_bloomer':
      if (metricValue) chips.push(compactNumber(metricValue));
      if (rankRecent) chips.push(`#${Math.round(rankRecent)} RECENT`);
      break;
    case 'B3_early_fade':
      if (metricValue) chips.push(compactNumber(metricValue));
      if (d1_pct) chips.push(`D1: P${Math.round(d1_pct)}`);
      break;
    case 'B4_sustained_strength':
      if (metricValue) chips.push(compactNumber(metricValue));
      if (d1_pct) chips.push(`D1:P${Math.round(d1_pct)} → D21:P${Math.round(curr_pct || asNumber(payload.d21) || percentile || d1_pct)}`);
      break;
    case 'D1_anchor_displacement':
      if (metricValue) chips.push(compactNumber(metricValue));
      if (percentile) chips.push(`TOP ${Math.round(percentile)}%`);
      break;
    case 'D2_competitive_gap_closing':
      if (gap) chips.push(`${Math.round(gap)} POS SHIFT`);
      if (percentile) chips.push(`TOP ${Math.round(percentile)}%`);
      break;
    case 'D3_competitor_regime_break':
      if (gap) chips.push(`${Math.round(gap)} POS SHIFT`);
      if (percentile) chips.push(`TOP ${Math.round(percentile)}%`);
      break;
    case 'E1_format_takeover':
      if (payload.reel_hot && payload.total_hot) chips.push(`${payload.reel_hot} OF ${payload.total_hot} HOT`);
      chips.push('FORMAT SHIFT');
      break;
    case 'E3_structural_convergence':
      chips.push('FEED CLUSTER');
      chips.push('CONVERGENCE');
      break;
    default:
      if (percentile) chips.push(`TOP ${Math.round(percentile)}%`);
      if (metricValue) chips.push(compactNumber(metricValue));
      break;
  }

  // DEDUPLICATION: Do not show data that is already in top chips, title, or body
  const textContext = renderedText.toLowerCase();
  const topTags = topBadgeTags.join(' ').toLowerCase();

  const dedupedChips = chips.filter(chip => {
    const c = chip.toLowerCase();
    
    // Direct match inside top tag strings
    if (topTags.includes(c)) return false;
    
    // Check if the specific number (e.g. 14k or 6) is inside top tags or text Context
    const numbersInChip = chip.match(/\d+(?:\.\d+)?[kKmM%]?/g) || [];
    for (const num of numbersInChip) {
      if (topTags.includes(num.toLowerCase())) return false;
      // Also catch "12" if text context says "top 12%"
      if (textContext.includes(num.toLowerCase())) return false;
    }
    
    return true;
  });

  // Unique elements only, fallback to empty array
  return Array.from(new Set(dedupedChips)).slice(0, 2);
}

function mapAlertRowToFireItem(row: AlertRow, mediaTypeByPostKey: Record<string, string>): FireItem {
  const payload = asRecord(row.payload);
  const alertType = asString(row.alert_type || 'spark');
  const urgency = toUrgency(alertType);

  const signalCode = asString(row.signal_code || payload.signal_code || '');
  const percentileRaw = asString(payload.percentile) || asString(payload.percentile_performance) || '';
  const pNum = asNumber(percentileRaw);

  const checkpointRaw = asString(payload.checkpoint) || asString(row.checkpoint) || 'd1';
  const businessDateKey = asString(payload.business_date_ist) || asString(row.business_date_ist) || toIstDateKey(row.created_at);
  const deltaNumber = asNumber(payload.delta ?? payload.delta_cp ?? payload.delta_vs_d1);
  const mappedMedia = row.post_key ? asString(mediaTypeByPostKey[row.post_key]) : '';
  const mediaType = normalizeMediaLabel(asString(payload.media_type || payload.post_type || payload.media || mappedMedia || inferMediaFromUrl(asString(payload.post_url)) || 'POST'));

  const metric = metricFromPayload(signalCode, payload);
  const backendTitle = cleanCopy(asString(row.title));
  const backendBody = cleanCopy(asString(row.body));

  const title = backendTitle || cleanCopy(asString(payload.headline)) || `${alertType.toUpperCase()} signal`;
  
  // New backend fn_compose_alert_body ensures whyNow is accurate, no heuristics needed
  const whyNow = backendBody || 'Review and execute next move.';

  const topDelta = formatDelta(deltaNumber, checkpointRaw);
  const topPct = asString(payload.percentile_tag) || percentileToTag(pNum);
  const topTags = [topDelta, topPct].filter(Boolean) as string[];

  const evidence = buildEvidenceChips(payload, signalCode, topTags, `${title} ${whyNow}`);

  return {
    id: `alert-${row.id}`,
    family: 'insight',
    urgency,
    color: '#FF6B00',
    handle: asString(payload.handle) ? `@${asString(payload.handle).replace(/^@+/, '')}` : '@feed',
    title,
    whyNow,
    action: asString(payload.action) || 'Review and execute next move.',
    percentileTag: asString(payload.percentile_tag) || percentileToTag(pNum),
    mediaType,
    stage: checkpointRaw.toUpperCase(),
    percentile: percentileRaw || undefined,
    delta: formatDelta(deltaNumber, checkpointRaw),
    evidence,
    timeAgo: timeAgoText(row.created_at),
    createdAt: row.created_at,
    postUrl: asString(payload.post_url) || 'https://instagram.com',
    thumbnailUrl: toMediaProxyUrl(asString(payload.thumbnail_url) || asString(payload.display_url)),
    businessDateKey,
  };
}

function istNowDate(): Date {
  const key = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(new Date());
  return new Date(`${key}T00:00:00`);
}

function sevenDayWindowRecentLeftToRight(): Date[] {
  const todayIst = istNowDate();
  const yesterdayIst = subDays(todayIst, 1);
  return Array.from({ length: 7 }, (_, i) => subDays(yesterdayIst, i));
}

function tickerFromItems(items: FireItem[]): TickerEntry[] {
  return items.slice(0, 30).map((item, idx) => {
    const pctText = item.percentile ? `P${item.percentile}` : 'P--';
    const deltaText = item.delta || (item.stage === 'D1' ? 'OPEN' : '--');
    const tone: 'up' | 'down' | 'flat' = deltaText.startsWith('+') ? 'up' : deltaText.startsWith('-') ? 'down' : 'flat';
    return {
      id: `${item.id}-${idx}`,
      text: `${item.handle} ${item.title} ${item.stage} ${pctText} ${deltaText} ${item.percentileTag}`,
      tone,
    };
  });
}

export default function FirePage() {
  const [items, setItems] = useState<FireItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [days] = useState<Date[]>(sevenDayWindowRecentLeftToRight());
  const [activeDate, setActiveDate] = useState<Date>(days[0]);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const supabase = createBrowserClient(
          process.env.NEXT_PUBLIC_SUPABASE_URL!,
          process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
        );

        const yesterday = subDays(istNowDate(), 1);
        const startDay = subDays(yesterday, 6);
        const yesterdayKey = format(yesterday, 'yyyy-MM-dd');
        const startDayKey = format(startDay, 'yyyy-MM-dd');

        const alertsRes = await supabase
          .from('fire_alerts')
          .select('id,alert_type,signal_code,title,body,payload,created_at,status,business_date_ist,checkpoint,post_key')
          .in('status', ['new', 'sent'])
          .gte('business_date_ist', startDayKey)
          .lte('business_date_ist', yesterdayKey)
          .order('created_at', { ascending: false })
          .limit(500);

        if (alertsRes.error) throw alertsRes.error;

        const rows = ((alertsRes.data as AlertRow[] | null) ?? []);

        const postKeys = Array.from(new Set(rows.map((r) => r.post_key).filter(Boolean))) as string[];
        const mediaTypeByPostKey: Record<string, string> = {};
        if (postKeys.length > 0) {
          const postsRes = await supabase
            .from('posts')
            .select('post_key,media_type')
            .in('post_key', postKeys);
          if (!postsRes.error) {
            for (const pr of (postsRes.data ?? []) as Array<{ post_key: string | null; media_type: string | null }>) {
              if (pr.post_key && pr.media_type) mediaTypeByPostKey[pr.post_key] = pr.media_type;
            }
          }
        }

        const mapped = rows.map((row) => mapAlertRowToFireItem(row, mediaTypeByPostKey));
        setItems(mapped);

        if (mapped.length > 0) {
          const latestKey = mapped
            .map((x) => x.businessDateKey)
            .sort((a, b) => b.localeCompare(a))[0];
          if (latestKey) setActiveDate(new Date(`${latestKey}T00:00:00`));
        }
      } catch (e) {
        console.error(e);
        setItems([]);
      } finally {
        setLoading(false);
      }
    }

    void load();
  }, []);

  const dayCounts = useMemo(() => {
    const counts: Record<string, { spark: number; burn: number; blaze: number }> = {};
    for (const d of days) counts[format(d, 'yyyy-MM-dd')] = { spark: 0, burn: 0, blaze: 0 };
    for (const item of items) {
      const key = item.businessDateKey;
      if (!counts[key]) counts[key] = { spark: 0, burn: 0, blaze: 0 };
      if (item.urgency === 'watch') counts[key].spark += 1;
      else if (item.urgency === 'today') counts[key].burn += 1;
      else counts[key].blaze += 1;
    }
    return counts;
  }, [items, days]);

  const activeKey = format(activeDate, 'yyyy-MM-dd');
  const activeItems = useMemo(() => items.filter((i) => i.businessDateKey === activeKey), [items, activeKey]);

  const blazeItems = activeItems.filter((i) => i.urgency === 'now');
  const burnItems = activeItems.filter((i) => i.urgency === 'today');
  const sparkItems = activeItems.filter((i) => i.urgency === 'watch');

  const tickerItems = useMemo(() => tickerFromItems(items), [items]);

  return (
    <div className="h-[100dvh] w-full bg-background overflow-y-scroll pb-32">
      <div className="min-h-[20vh] flex flex-col justify-end px-4 pb-6 pt-20">
        <h1 className="text-[12vw] font-black uppercase tracking-tighter leading-[0.85] flex flex-col">
          <span>FIRE</span>
          <span className="text-[4vw] tracking-normal font-bold text-neutral-400 stroke-text">INTELLIGENCE</span>
        </h1>
      </div>

      <StatsTicker entries={tickerItems} />

      <div className="px-4">
        <DayPicker days={days} activeDate={activeDate} onSelect={setActiveDate} dayCounts={dayCounts} />
      </div>

      <div className="px-4 mt-6">
        <FireOverview total={activeItems.length} spark={sparkItems.length} burn={burnItems.length} blaze={blazeItems.length} />
      </div>

      {loading ? (
        <div className="h-[40vh] flex items-center justify-center">
          <Loader2 className="animate-spin w-12 h-12" />
        </div>
      ) : (
        <div className="px-4 space-y-8">
          {blazeItems.length > 0 && (
            <section>
              <h2 className="text-2xl font-black uppercase mb-3">BLAZE</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {blazeItems.map((item) => <FireCard key={item.id} item={item} />)}
              </div>
            </section>
          )}

          {burnItems.length > 0 && (
            <section>
              <h2 className="text-2xl font-black uppercase mb-3">BURN</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {burnItems.map((item) => <FireCard key={item.id} item={item} />)}
              </div>
            </section>
          )}

          {sparkItems.length > 0 && (
            <section>
              <h2 className="text-2xl font-black uppercase mb-3">SPARK</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {sparkItems.map((item) => <FireCard key={item.id} item={item} />)}
              </div>
            </section>
          )}

          {!loading && activeItems.length === 0 && (
            <div className="text-center py-20 opacity-50">
              <h3 className="text-2xl font-black uppercase">System Cold</h3>
              <p>No signals found.</p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
