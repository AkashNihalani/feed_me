'use client';

/* eslint-disable @next/next/no-img-element -- pool thumbnails are dynamic feed assets, not next/image candidates */

import { type CSSProperties, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, LayoutGroup, motion, type Variants } from 'framer-motion';
import { ChevronRight, ExternalLink, X } from 'lucide-react';
import { FeatureCarousel, type FeatureCarouselImage } from '@/components/ui/feature-carousel';
import { GRID_ITEM_EASE, GRID_LAYOUT_SPRING } from '@/lib/motion';
import { cn } from '@/lib/utils';

// ── types ───────────────────────────────────────────────────────────────────

type FeederPoolPayload = {
  accounts: FeederPoolAccount[];
  error?: string;
};

type FeederPoolAccount = {
  feeder: {
    handle: string;
    display_handle: string;
    media_type: string;
    window_days: number;
  };
  header: {
    state_label: string;
    meta_line: string;
  };
  blocks: FeederPoolBlock[];
  candidate_blocks: FeederPoolBlock[];
};

type FeederPoolBlock = {
  pool_id: string;
  pool_slug: string;
  kind: 'pool' | 'candidate';
  headline: string;
  why_this_pool_exists?: string;
  description_sentences?: string[];
  metrics?: PoolMetrics;
  rank_inputs?: { pool_pressure_score?: number | null };
  frontend_block?: {
    metric_line?: string | null;
    variant?: 'hero' | 'large' | 'medium' | 'small' | 'candidate';
    relative_area?: number | null;
    layout_id?: string;
  };
  frontend_copy?: {
    cover?: {
      headline_full?: string | null;
      headline_compact?: string | null;
      eyebrow?: string | null;
      one_line_read?: string | null;
      metric_support?: string | null;
    };
    popup?: {
      opening_read?: string | null;
      repeatable_sequence?: ReasoningCards | null;
      why_the_posts_belong_together?: string[];
      performance_read?: string | null;
      implementation_read?: string | null;
    };
  };
  rank?: {
    rank_among_all_blocks?: number | null;
    rank_among_active_pools?: number | null;
  };
  members: FeederPoolMember[];
};

type PoolMetrics = {
  member_count?: number | null;
  best_top_label?: string | null;
  average_top_percent?: number | null;
  metric_mix?: Record<string, number>;
  leader_post_key?: string | null;
  leader_metric?: string | null;
  leader_multiple?: number | null;
};

type FeederPoolMember = {
  post_key: string;
  post_url?: string | null;
  thumbnail_url?: string | null;
  caption?: string | null;
  metrics?: PostMetrics;
  rank?: { rank_within_pool?: number | null };
  backend_intelligence?: {
    expanded_summary?: string | null;
    expanded_pool_reasoning?: string | null;
  };
  yaml_verbatim?: {
    summary?: string | null;
    why_it_clusters_here?: string | null;
    receipts?: string[];
  };
  frontend_detail_packaging?: {
    scan_layer?: string | null;
    evidence_layer?: {
      raw?: string[];
      timeline?: Array<{ timestamp?: string | null; label?: string | null; text?: string | null }>;
      key_moments?: Array<{ timestamp?: string | null; label?: string | null }>;
    };
    depth_layer?: {
      expanded_backend_summary?: string | null;
      summary_verbatim?: string | null;
    };
  };
  frontend_copy?: {
    role_in_pool?: string | null;
    selected_proof_line?: string | null;
    reasoning_cards?: ReasoningCards | null;
    receipt_chips?: string[];
  };
};

type ReasoningCards = {
  entry?: string | null;
  progression?: string | null;
  shift?: string | null;
  ending?: string | null;
};

type PostMetrics = {
  best_metric?: string | null;
  best_top_label?: string | null;
  best_multiple_label?: string | null;
  current_value_labels?: { views?: string | null; likes?: string | null; comments?: string | null };
};

type FeederPoolHeroProps = { selectedHandle?: string };

const ACCOUNT_ORDER = ['saniyamirwani', 'lakmeindia', 'kaybykatrina', 'trysugar'];

const POST_SWAP_CONTAINER_VARIANTS: Variants = {
  hidden: { opacity: 0, y: 18, scale: 0.985, filter: 'blur(12px)' },
  show: {
    opacity: 1,
    y: 0,
    scale: 1,
    filter: 'blur(0px)',
    transition: {
      type: 'spring',
      stiffness: 255,
      damping: 24,
      mass: 0.82,
      delayChildren: 0.035,
      staggerChildren: 0.055,
    },
  },
  exit: {
    opacity: 0,
    y: -10,
    scale: 0.985,
    filter: 'blur(8px)',
    transition: { duration: 0.2, ease: GRID_ITEM_EASE },
  },
};

const POST_SWAP_ITEM_VARIANTS: Variants = {
  hidden: { opacity: 0, y: 16, scale: 0.975, filter: 'blur(10px)' },
  show: {
    opacity: 1,
    y: 0,
    scale: 1,
    filter: 'blur(0px)',
    transition: { type: 'spring', stiffness: 340, damping: 27, mass: 0.72 },
  },
  exit: {
    opacity: 0,
    y: -8,
    scale: 0.985,
    filter: 'blur(7px)',
    transition: { duration: 0.16, ease: GRID_ITEM_EASE },
  },
};

const POST_SWAP_TILE_VARIANTS: Variants = {
  hidden: { opacity: 0, y: 14, scale: 0.94 },
  show: {
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { type: 'spring', stiffness: 420, damping: 24, mass: 0.7 },
  },
};

// ── helpers ─────────────────────────────────────────────────────────────────

function normalizeHandle(value: string | null | undefined): string {
  return String(value || '').trim().replace(/^@+/, '').toLowerCase();
}

function hashSeed(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) hash = (hash * 31 + value.charCodeAt(index)) % 10_000;
  return hash;
}

function sortedBlocks(blocks: FeederPoolBlock[]): FeederPoolBlock[] {
  return [...blocks].sort((a, b) => {
    const rankA = a.rank?.rank_among_all_blocks;
    const rankB = b.rank?.rank_among_all_blocks;
    if (typeof rankA === 'number' && typeof rankB === 'number') return rankA - rankB;
    return (b.rank_inputs?.pool_pressure_score || 0) - (a.rank_inputs?.pool_pressure_score || 0);
  });
}

function poolSignal(block: FeederPoolBlock): string {
  const mix = block.metrics?.metric_mix || {};
  const ranked = Object.entries(mix).sort((a, b) => b[1] - a[1]);
  const dominant = ranked[0]?.[0];
  if (!dominant || dominant === 'rank_only') return 'rank-led';
  return `${dominant}-led`;
}

function humanSignal(block: FeederPoolBlock): string {
  return poolSignal(block).replace(/-/g, ' ');
}

function signalDominant(block: FeederPoolBlock): string {
  const mix = block.metrics?.metric_mix || {};
  const ranked = Object.entries(mix).sort((a, b) => b[1] - a[1]);
  return ranked[0]?.[0] || 'rank';
}

function poolSupportLabel(block: FeederPoolBlock): string {
  const count = block.metrics?.member_count || block.members.length;
  return `${count} ${count === 1 ? 'post' : 'posts'}`;
}

function leaderMember(block: FeederPoolBlock): FeederPoolMember | null {
  const leaderKey = block.metrics?.leader_post_key;
  if (leaderKey) {
    const match = block.members.find((member) => member.post_key === leaderKey);
    if (match) return match;
  }
  return block.members[0] || null;
}

function memberMetricLine(member: FeederPoolMember | null): string {
  if (!member) return '';
  const metrics = member.metrics || {};
  const views = metrics.current_value_labels?.views ? `${metrics.current_value_labels.views} views` : null;
  return [metrics.best_top_label, metrics.best_metric, metrics.best_multiple_label, views]
    .filter(Boolean)
    .join(' · ');
}

function memberMetricParts(member: FeederPoolMember | null): string[] {
  return memberMetricLine(member)
    .split(' · ')
    .map((part) => part.trim())
    .filter(Boolean);
}

function thumbnailFor(member: FeederPoolMember | null): string {
  if (!member) return '';
  const explicit = (member.thumbnail_url || '').trim();
  if (explicit && !explicit.includes('/api/media?')) return explicit;
  const key = (member.post_key || '').trim();
  if (!key) return '';
  return `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail`;
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function extractSequence(summary: string | null | undefined) {
  const text = summary || '';
  const labels = [
    { label: 'Entry', source: 'Fingerprint entry state' },
    { label: 'Progression', source: 'Progression' },
    { label: 'Shift', source: 'Shift' },
    { label: 'Ending', source: 'Ending state' },
  ];

  return labels
    .map((item) => {
      const otherLabels = labels.map((entry) => escapeRegExp(entry.source)).join('|');
      const pattern = new RegExp(`${escapeRegExp(item.source)}:\\s*([\\s\\S]*?)(?=\\n\\s*(?:${otherLabels}):|$)`, 'i');
      const match = text.match(pattern);
      return {
        label: item.label,
        text: (match?.[1] || '').trim(),
      };
    })
    .filter((item) => item.text.length > 0);
}

function sequenceFromCards(cards: ReasoningCards | null | undefined) {
  if (!cards) return [];
  return [
    { label: 'Entry', text: cards.entry || '' },
    { label: 'Progression', text: cards.progression || '' },
    { label: 'Shift', text: cards.shift || '' },
    { label: 'Ending', text: cards.ending || '' },
  ].filter((item) => item.text.trim().length > 0);
}

function shortHeadlineMeta(block: FeederPoolBlock): string {
  // Compact one-liner shown above headline on the tile (eyebrow).
  return [poolSupportLabel(block), block.metrics?.best_top_label].filter(Boolean).join(' · ');
}

function carouselImagesForBlock(block: FeederPoolBlock): FeatureCarouselImage[] {
  return (block.members || []).map((member, index) => ({
    src: thumbnailFor(member),
    alt: `${block.headline} evidence ${index + 1}`,
    label: member.rank?.rank_within_pool ? `proof #${member.rank.rank_within_pool}` : `proof ${index + 1}`,
  }));
}

function compactPostLabel(member: FeederPoolMember | null): string {
  return (member?.post_key || 'post').split('#')[0].replace(/^p\//, '').slice(0, 10).toUpperCase();
}

function blockLayoutId(block: FeederPoolBlock): string {
  return block.frontend_block?.layout_id || block.pool_slug || block.pool_id;
}

function ThumbnailFallback({ label }: { label: string }) {
  return (
    <div className="flex h-full w-full flex-col justify-between bg-[radial-gradient(circle_at_25%_18%,rgba(225,29,72,0.2),transparent_38%),linear-gradient(145deg,rgba(0,0,0,0.05),rgba(0,0,0,0.015))] p-3 dark:bg-[radial-gradient(circle_at_25%_18%,rgba(225,29,72,0.26),transparent_38%),linear-gradient(145deg,rgba(255,255,255,0.07),rgba(255,255,255,0.02))]">
      <div className="w-fit rounded-full border border-foreground/[0.08] bg-foreground/[0.035] px-2 py-1 text-[7px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:border-white/[0.1] dark:bg-white/[0.05] dark:text-white/42">
        Preview pending
      </div>
      <div className="text-[9px] font-black uppercase tracking-[0.1em] text-foreground/34 dark:text-white/34">
        {label}
      </div>
    </div>
  );
}

function ThumbImage({
  src,
  label,
  className,
}: {
  src: string;
  label: string;
  className?: string;
}) {
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  const failed = Boolean(src && failedSrc === src);

  if (!src || failed) return <ThumbnailFallback label={label} />;
  return <img src={src} alt="" loading="lazy" onError={() => setFailedSrc(src)} className={cn('h-full w-full object-cover', className)} />;
}

// ── viewport hook ───────────────────────────────────────────────────────────

function useInViewport(ref: RefObject<HTMLElement | null>): boolean {
  const [inView, setInView] = useState(true);
  useEffect(() => {
    const node = ref.current;
    if (!node || typeof IntersectionObserver === 'undefined') return undefined;
    const observer = new IntersectionObserver(
      (entries) => entries.forEach((entry) => setInView(entry.isIntersecting)),
      { rootMargin: '120px' },
    );
    observer.observe(node);
    return () => observer.disconnect();
  }, [ref]);
  return inView;
}

// ── pool tile (premium cover surface) ───────────────────────────────────────

function PoolTile({
  block,
  index,
  onOpen,
}: {
  block: FeederPoolBlock;
  index: number;
  onOpen: (block: FeederPoolBlock) => void;
}) {
  const containerRef = useRef<HTMLButtonElement | null>(null);
  const inView = useInViewport(containerRef);
  const coverCopy = block.frontend_copy?.cover;
  const headline = coverCopy?.headline_compact || coverCopy?.headline_full || block.headline;
  const eyebrow = coverCopy?.eyebrow || humanSignal(block);
  const metricSupport = coverCopy?.metric_support || shortHeadlineMeta(block);
  const oneLineRead = coverCopy?.one_line_read;
  const images = carouselImagesForBlock(block);
  const support = poolSupportLabel(block);
  const rank = block.rank?.rank_among_active_pools || block.rank?.rank_among_all_blocks;
  const bestTop = block.metrics?.best_top_label;
  const extraEvidence = Math.max(0, block.members.length - 3);

  return (
    <motion.button
      ref={containerRef}
      type="button"
      layout
      layoutId={blockLayoutId(block)}
      initial={{ opacity: 0, y: 28, scale: 0.94, filter: 'blur(8px)' }}
      animate={{ opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
      exit={{ opacity: 0, y: -12, scale: 0.968, filter: 'blur(4px)' }}
      whileHover={{ y: -2 }}
      whileTap={{ scale: 0.985 }}
      transition={{
        layout: GRID_LAYOUT_SPRING,
        opacity: { duration: 0.28, delay: Math.min(index * 0.055, 0.3), ease: GRID_ITEM_EASE },
        y: { type: 'spring', stiffness: 395, damping: 30, mass: 0.78, delay: Math.min(index * 0.055, 0.3) },
        scale: { duration: 0.32, delay: Math.min(index * 0.055, 0.3), ease: GRID_ITEM_EASE },
        filter: { duration: 0.28, delay: Math.min(index * 0.055, 0.3), ease: GRID_ITEM_EASE },
      }}
      onClick={() => onOpen(block)}
      className="fm-depth-glass group relative isolate flex min-h-[430px] w-full flex-col overflow-hidden rounded-[26px] p-0 text-left sm:col-span-1 sm:min-h-[560px] sm:rounded-[28px] lg:col-span-3 lg:min-h-[430px] xl:min-h-[460px]"
      aria-label={`Open pool: ${block.headline}`}
    >
      <div className="pointer-events-none absolute inset-0 z-0">
        <div className="absolute left-[-18%] top-[-28%] h-[340px] w-[70%] rounded-full bg-[radial-gradient(circle,rgba(225,29,72,0.28),transparent_64%)] blur-[10px]" />
        <div className="absolute bottom-[-32%] right-[-12%] h-[390px] w-[58%] rounded-full bg-[radial-gradient(circle,rgba(251,113,133,0.18),transparent_66%)] blur-[4px]" />
        <div className="absolute inset-0 bg-[linear-gradient(135deg,rgba(255,255,255,0.14),transparent_40%,rgba(225,29,72,0.07)_100%)] dark:bg-[linear-gradient(135deg,rgba(255,255,255,0.07),transparent_42%,rgba(0,0,0,0.46))]" />
      </div>

      <div className="relative z-10 flex flex-1 flex-col gap-y-4 px-4 py-4 sm:gap-y-5 sm:px-6 sm:py-6 lg:grid lg:grid-cols-[minmax(250px,0.9fr)_minmax(300px,1.1fr)] lg:grid-rows-1 lg:gap-x-0 lg:gap-y-0 lg:p-0">
        <div className="flex min-h-0 flex-col lg:px-6 lg:pb-6 lg:pt-6 lg:pr-0 xl:px-7 xl:py-7 xl:pr-0">
          <div className="flex items-start justify-between gap-3 lg:items-center">
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="inline-flex items-center rounded-full bg-[#E11D48] px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.14em] text-white shadow-[0_12px_24px_-16px_rgba(225,29,72,0.9)]">
                {eyebrow}
              </span>
              {metricSupport && (
                <span className="inline-flex max-w-[170px] items-center rounded-full border border-foreground/[0.08] bg-white/60 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.12em] text-foreground/54 backdrop-blur-[12px] dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/58 min-[430px]:max-w-[220px] sm:max-w-[320px] lg:max-w-[260px] xl:max-w-[340px]">
                  <span className="truncate">{metricSupport}</span>
                </span>
              )}
            </div>
            <span className="absolute right-4 top-4 z-20 flex h-9 w-9 shrink-0 items-center justify-center rounded-full border border-foreground/[0.08] bg-white/62 text-foreground/58 backdrop-blur-[12px] transition-transform group-hover:translate-x-0.5 dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/62 sm:h-10 sm:w-10 lg:static">
              <ChevronRight size={17} strokeWidth={2.7} />
            </span>
          </div>

          <h3 className="fm-depth-title mt-3 pr-2 text-[22px] font-extrabold leading-[1.04] tracking-[-0.01em] text-foreground [text-wrap:balance] dark:text-white min-[390px]:text-[24px] min-[430px]:text-[26px] sm:mt-8 sm:max-w-[520px] sm:text-[40px] sm:font-black sm:leading-[0.94] sm:tracking-normal lg:mt-10 lg:max-w-[430px] lg:pr-0 lg:text-[49px] lg:leading-[0.9] xl:text-[55px] 2xl:text-[58px]">
            {headline}
          </h3>

          {oneLineRead && (
            <p className="mt-2.5 pr-1 text-[13px] font-bold leading-[1.35] text-foreground/62 [text-wrap:balance] dark:text-white/62 min-[390px]:text-[14px] sm:mt-4 sm:max-w-[480px] sm:text-[16px] sm:font-black sm:leading-[1.28] lg:max-w-[400px] lg:pr-0 lg:text-[17px]">
              {oneLineRead}
            </p>
          )}

          <div className="mt-6 hidden grid-cols-3 gap-2 lg:grid">
            <div className="rounded-[14px] border border-foreground/[0.07] bg-white/44 px-3 py-3 backdrop-blur-[14px] dark:border-white/[0.07] dark:bg-white/[0.045]">
              <div className="text-[8px] font-black uppercase tracking-[0.17em] text-foreground/36 dark:text-white/34">Pool rank</div>
              <div className="mt-1.5 text-[22px] font-black leading-none text-foreground dark:text-white">{rank ? `#${rank}` : '—'}</div>
            </div>
            <div className="rounded-[14px] border border-foreground/[0.07] bg-white/44 px-3 py-3 backdrop-blur-[14px] dark:border-white/[0.07] dark:bg-white/[0.045]">
              <div className="text-[8px] font-black uppercase tracking-[0.17em] text-foreground/36 dark:text-white/34">Support</div>
              <div className="mt-1.5 text-[22px] font-black leading-none text-foreground dark:text-white">{support.replace(' posts', '').replace(' post', '')}</div>
            </div>
            <div className="rounded-[14px] border border-foreground/[0.07] bg-white/44 px-3 py-3 backdrop-blur-[14px] dark:border-white/[0.07] dark:bg-white/[0.045]">
              <div className="text-[8px] font-black uppercase tracking-[0.17em] text-foreground/36 dark:text-white/34">Best</div>
              <div className="mt-1.5 text-[16px] font-black leading-none tracking-normal text-foreground dark:text-white xl:text-[18px]">
                {bestTop || '—'}
              </div>
            </div>
          </div>

          {extraEvidence > 0 && (
            <div className="mt-auto hidden pt-5 text-[10px] font-black uppercase tracking-[0.16em] text-[#E11D48] dark:text-[#FB7185] lg:block">
              +{extraEvidence} more proof {extraEvidence === 1 ? 'card' : 'cards'} in rotation
            </div>
          )}
        </div>

        <div className="relative z-10 aspect-[4/3] min-h-0 self-stretch overflow-hidden rounded-[20px] sm:aspect-[16/9] sm:rounded-[22px] lg:aspect-auto lg:overflow-visible lg:rounded-none lg:py-7 lg:pl-0 lg:pr-5 xl:pr-6">
          <FeatureCarousel
            images={images}
            isPlaying={inView}
            intervalMs={3600 + (hashSeed(block.pool_id) % 1000)}
            density="cover"
            className="h-full min-h-0 lg:min-h-[370px] xl:min-h-[400px]"
          />
        </div>

        <div className="grid grid-cols-3 gap-1.5 rounded-[18px] border border-foreground/[0.07] bg-white/44 p-1.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.32)] backdrop-blur-[14px] dark:border-white/[0.07] dark:bg-white/[0.045] lg:hidden">
          <div className="rounded-[14px] bg-foreground/[0.025] px-2.5 py-2.5 dark:bg-white/[0.035]">
            <div className="text-[7.5px] font-black uppercase tracking-[0.16em] text-foreground/40 dark:text-white/38">Rank</div>
            <div className="mt-1.5 text-[20px] font-black leading-none text-foreground dark:text-white">{rank ? `#${rank}` : '—'}</div>
          </div>
          <div className="rounded-[14px] bg-foreground/[0.025] px-2.5 py-2.5 dark:bg-white/[0.035]">
            <div className="text-[7.5px] font-black uppercase tracking-[0.16em] text-foreground/40 dark:text-white/38">Proof</div>
            <div className="mt-1.5 text-[20px] font-black leading-none text-foreground dark:text-white">{support.replace(' posts', '').replace(' post', '')}</div>
          </div>
          <div className="rounded-[14px] border border-[#E11D48]/18 bg-[#E11D48]/10 px-2.5 py-2.5">
            <div className="text-[7.5px] font-black uppercase tracking-[0.16em] text-[#E11D48] dark:text-[#FDA4AF]">Best</div>
            <div className="mt-1.5 text-[15px] font-black leading-none tracking-normal text-foreground dark:text-white">{bestTop || '—'}</div>
          </div>
        </div>
      </div>
    </motion.button>
  );
}

// ── candidate chip (small horizontal rail item) ─────────────────────────────

function CandidateChip({
  block,
  index,
  onOpen,
}: {
  block: FeederPoolBlock;
  index: number;
  onOpen: (block: FeederPoolBlock) => void;
}) {
  const thumb = thumbnailFor(block.members[0]);
  const coverCopy = block.frontend_copy?.cover;
  const eyebrow = coverCopy?.eyebrow || 'Watching';
  const oneLineRead = coverCopy?.one_line_read;
  return (
    <motion.button
      type="button"
      layout
      layoutId={blockLayoutId(block)}
      initial={{ opacity: 0, y: 18, scale: 0.95, filter: 'blur(7px)' }}
      animate={{ opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
      exit={{ opacity: 0, y: -8, scale: 0.97, filter: 'blur(4px)' }}
      transition={{
        layout: GRID_LAYOUT_SPRING,
        opacity: { duration: 0.24, delay: Math.min(index * 0.035, 0.24), ease: GRID_ITEM_EASE },
        y: { type: 'spring', stiffness: 340, damping: 28, mass: 0.78, delay: Math.min(index * 0.035, 0.24) },
        scale: { duration: 0.24, delay: Math.min(index * 0.035, 0.24), ease: GRID_ITEM_EASE },
        filter: { duration: 0.2, delay: Math.min(index * 0.035, 0.24), ease: GRID_ITEM_EASE },
      }}
      whileTap={{ scale: 0.975 }}
      onClick={() => onOpen(block)}
      className="fm-depth-glass group relative grid w-[260px] shrink-0 grid-cols-[64px_minmax(0,1fr)] gap-3 overflow-hidden rounded-[18px] p-2.5 text-left"
    >
      <div className="relative aspect-[4/5] overflow-hidden rounded-[12px] border border-foreground/[0.08] bg-foreground/[0.04] dark:border-white/[0.1] dark:bg-white/[0.04]">
        <ThumbImage src={thumb} label={compactPostLabel(block.members[0])} />
      </div>
      <div className="min-w-0">
        <div className="flex items-center gap-1.5 text-[8px] font-black uppercase tracking-[0.16em] text-foreground/48 dark:text-white/42">
          <span className="h-1 w-1 rounded-full bg-[#E11D48]" />
          {eyebrow}
        </div>
        <div className="mt-1.5 line-clamp-3 text-[14px] font-black leading-[1.06] tracking-normal text-foreground [text-wrap:balance] dark:text-white">
          {block.headline}
        </div>
        {oneLineRead && (
          <div className="mt-1.5 line-clamp-2 text-[10.5px] font-bold leading-[1.22] text-foreground/50 dark:text-white/46">
            {oneLineRead}
          </div>
        )}
      </div>
    </motion.button>
  );
}

// ── dialog: featured preview + member rail + stat row + content ─────────────

function FeaturedThumbnail({ member }: { member: FeederPoolMember | null }) {
  const targetSrc = thumbnailFor(member);
  const [visible, setVisible] = useState<string>(targetSrc);
  const [incoming, setIncoming] = useState<string | null>(null);
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  const failed = Boolean(targetSrc && failedSrc === targetSrc);

  useEffect(() => {
    if (targetSrc === visible) return undefined;
    if (!targetSrc) {
      const frame = window.requestAnimationFrame(() => setVisible(targetSrc));
      return () => window.cancelAnimationFrame(frame);
    }
    const image = new window.Image();
    image.onload = () => setIncoming(targetSrc);
    image.onerror = () => {
      setIncoming(null);
      setVisible('');
      setFailedSrc(targetSrc);
    };
    image.src = targetSrc;
    return undefined;
  }, [targetSrc, visible]);

  return (
    <div className="relative h-full w-full overflow-hidden rounded-[22px] border border-foreground/[0.08] bg-[radial-gradient(circle_at_28%_20%,rgba(225,29,72,0.12),transparent_42%),linear-gradient(135deg,rgba(0,0,0,0.04),rgba(0,0,0,0.015))] dark:border-white/[0.08] dark:bg-[radial-gradient(circle_at_28%_20%,rgba(225,29,72,0.18),transparent_42%),linear-gradient(135deg,rgba(255,255,255,0.08),rgba(255,255,255,0.025))]">
      {visible && !failed && (
        <motion.img
          key={visible}
          src={visible}
          alt=""
          className="absolute inset-0 h-full w-full object-cover"
          initial={false}
          animate={{ opacity: 1 }}
          onError={() => {
            setFailedSrc(visible);
            setVisible('');
          }}
        />
      )}
      {incoming && incoming !== visible && (
        <motion.img
          key={incoming}
          src={incoming}
          alt=""
          className="absolute inset-0 h-full w-full object-cover"
          initial={{ opacity: 0, scale: 1.02, filter: 'blur(20px)' }}
          animate={{ opacity: 1, scale: 1, filter: 'blur(0px)' }}
          transition={{ duration: 1.6, ease: [0.16, 1, 0.3, 1] }}
          onAnimationComplete={() => {
            setVisible(incoming);
            setIncoming(null);
          }}
        />
      )}
      {(!visible || failed) && <ThumbnailFallback label={compactPostLabel(member)} />}
    </div>
  );
}

function MemberRail({
  members,
  selectedKey,
  onSelect,
}: {
  members: FeederPoolMember[];
  selectedKey: string | null;
  onSelect: (key: string) => void;
}) {
  const railRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    if (!selectedKey) return;
    const target = railRef.current?.querySelector<HTMLElement>(`[data-rail-key="${CSS.escape(selectedKey)}"]`);
    target?.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
  }, [selectedKey]);

  if (members.length === 0) return null;
  return (
    <div ref={railRef} className="hide-scrollbar -mx-1 flex snap-x gap-2 overflow-x-auto px-1 py-2">
      {members.map((member) => {
        const isActive = member.post_key === selectedKey;
        const thumb = thumbnailFor(member);
        return (
          <motion.button
            key={member.post_key}
            type="button"
            data-rail-key={member.post_key}
            onClick={() => onSelect(member.post_key)}
            className={cn(
              'group relative h-[88px] w-[70px] min-w-[70px] shrink-0 snap-center overflow-hidden rounded-[14px] border bg-foreground/[0.035] transition dark:bg-white/[0.04]',
              isActive
                ? 'border-[#E11D48]/64 opacity-100 shadow-[0_16px_36px_rgba(225,29,72,0.18)] dark:border-[#FB7185]/68 dark:shadow-[0_16px_36px_rgba(225,29,72,0.2)]'
                : 'border-foreground/[0.08] opacity-60 hover:opacity-100 dark:border-white/[0.08]',
            )}
            animate={{ scale: isActive ? 1 : 0.965 }}
            transition={{ type: 'spring', stiffness: 380, damping: 26, mass: 0.7 }}
          >
            {isActive && (
              <motion.div
                layoutId="pool-rail-active"
                className="pointer-events-none absolute inset-0 z-20 rounded-[14px] ring-2 ring-[#E11D48]/72 ring-offset-2 ring-offset-[#f7f7f7] dark:ring-[#FB7185]/78 dark:ring-offset-[#080808]"
                transition={{ type: 'spring', stiffness: 360, damping: 34, mass: 0.8 }}
              />
            )}
            <ThumbImage src={thumb} label={compactPostLabel(member)} />
          </motion.button>
        );
      })}
    </div>
  );
}

function SelectedProofDock({
  member,
  index,
  total,
  mediaType,
}: {
  member: FeederPoolMember | null;
  index: number;
  total: number;
  mediaType: string;
}) {
  const metricParts = memberMetricParts(member);
  const postLabel = compactPostLabel(member);
  const role = member?.frontend_copy?.role_in_pool;
  const metrics = member?.metrics || {};
  const topLabel = metrics.best_top_label || metricParts[0] || 'Top --';
  const leadMetric = metrics.best_metric || mediaType || 'post';
  const baseline = metrics.best_multiple_label || null;
  const views = metrics.current_value_labels?.views ? `${metrics.current_value_labels.views} views` : null;
  const secondaryStats = [
    baseline ? { label: 'Baseline', value: baseline } : null,
    views ? { label: 'Views', value: views } : null,
    metrics.current_value_labels?.likes ? { label: 'Likes', value: metrics.current_value_labels.likes } : null,
    metrics.current_value_labels?.comments ? { label: 'Comments', value: metrics.current_value_labels.comments } : null,
  ].filter(Boolean) as Array<{ label: string; value: string }>;

  return (
    <div className="rounded-[24px] border border-foreground/[0.08] bg-white/68 p-3.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.62),0_24px_58px_-38px_rgba(15,23,42,0.64)] backdrop-blur-[18px] dark:border-white/[0.08] dark:bg-white/[0.055] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.075),0_24px_58px_-38px_rgba(0,0,0,0.98)] lg:p-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]/82 dark:text-[#FB7185]/82">
            Selected proof
          </span>
          <span className="rounded-full border border-foreground/[0.08] bg-foreground/[0.035] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/46 dark:border-white/[0.08] dark:bg-white/[0.055] dark:text-white/46">
            {postLabel}
          </span>
        </div>
        {member?.post_url && (
          <a
            href={member.post_url}
            target="_blank"
            rel="noreferrer"
            className="inline-flex shrink-0 items-center gap-1.5 rounded-full bg-[#E11D48] px-3.5 py-2.5 text-[8.5px] font-black uppercase tracking-[0.14em] text-white shadow-[0_16px_30px_-18px_rgba(225,29,72,0.95)] transition hover:translate-y-[-1px] hover:bg-[#be123c] lg:px-4"
          >
            Open <ExternalLink size={12} strokeWidth={2.8} />
          </a>
        )}
      </div>

      <div className="mt-3 grid grid-cols-[0.82fr_1.18fr] gap-2">
        <div className="rounded-[19px] border border-foreground/[0.06] bg-foreground/[0.025] px-3 py-3.5 dark:border-white/[0.07] dark:bg-white/[0.04]">
          <div className="text-[8px] font-black uppercase tracking-[0.17em] text-foreground/38 dark:text-white/34">
            Proof
          </div>
          <div className="mt-2 text-[30px] font-black leading-none tracking-normal text-foreground dark:text-white lg:text-[34px]">
            {index}/{total}
          </div>
          {role && (
            <div className="mt-2.5 text-[9px] font-black uppercase leading-[1.25] tracking-[0.12em] text-foreground/50 dark:text-white/46">
              {role}
            </div>
          )}
        </div>

        <div className="rounded-[19px] border border-[#E11D48]/24 bg-[linear-gradient(135deg,rgba(225,29,72,0.16),rgba(225,29,72,0.055))] px-3.5 py-3.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.3)]">
          <div className="text-[8px] font-black uppercase tracking-[0.17em] text-[#E11D48] dark:text-[#FDA4AF]">
            Best rank
          </div>
          <div className="mt-2 text-[31px] font-black leading-[0.94] tracking-normal text-foreground dark:text-white lg:text-[36px]">
            {topLabel}
          </div>
          <div className="mt-2 inline-flex rounded-full bg-[#E11D48]/12 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.13em] text-[#E11D48] dark:bg-[#FB7185]/14 dark:text-[#FDA4AF]">
            {leadMetric} led
          </div>
        </div>
      </div>

      {secondaryStats.length > 0 && (
        <div className="mt-2 grid grid-cols-2 gap-1.5">
          {secondaryStats.slice(0, 4).map((stat) => (
            <div
              key={`${stat.label}-${stat.value}`}
              className="min-w-0 rounded-[15px] border border-foreground/[0.07] bg-foreground/[0.025] px-2.5 py-2.5 dark:border-white/[0.07] dark:bg-white/[0.04]"
            >
              <div className="text-[7.5px] font-black uppercase tracking-[0.14em] text-foreground/34 dark:text-white/34">
                {stat.label}
              </div>
              <div className="mt-1 truncate text-[13px] font-black leading-none tracking-normal text-foreground/76 dark:text-white/76">
                {stat.value}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// Bold three-up stat row at the top of the popup content
function StatRow({ block }: { block: FeederPoolBlock }) {
  const member_count = block.metrics?.member_count || block.members.length;
  const top = block.metrics?.best_top_label || '—';
  const leader = block.metrics?.leader_multiple;
  const leaderLabel = typeof leader === 'number' && Number.isFinite(leader) ? `${leader.toFixed(1)}× baseline` : null;
  const dominant = signalDominant(block);

  const cells: Array<{ label: string; value: string; sub?: string | null; emphasis?: boolean }> = [
    {
      label: 'Strongest',
      value: top,
      sub: leaderLabel,
      emphasis: true,
    },
    {
      label: 'Support',
      value: String(member_count),
      sub: member_count === 1 ? 'post' : 'posts',
    },
    {
      label: 'Signal',
      value: dominant,
      sub: 'led',
    },
  ];

  return (
    <div className="grid grid-cols-3 gap-2">
      {cells.map((cell) => (
        <div
          key={cell.label}
          className={cn(
            'min-h-[82px] rounded-[18px] border px-3.5 py-3.5 lg:min-h-[96px] lg:px-4 lg:py-4',
            cell.emphasis
              ? 'border-[#E11D48]/22 bg-[#E11D48]/9 shadow-[inset_0_1px_0_rgba(255,255,255,0.18)]'
              : 'border-foreground/[0.08] bg-foreground/[0.025] dark:border-white/[0.08] dark:bg-white/[0.035]',
          )}
        >
          <div
            className={cn(
              'text-[8.5px] font-black uppercase tracking-[0.18em] md:text-[9px]',
              cell.emphasis ? 'text-[#E11D48] dark:text-[#FDA4AF]' : 'text-foreground/38 dark:text-white/34',
            )}
          >
            {cell.label}
          </div>
          <div
            className={cn(
              'mt-2 font-black leading-[0.92] tracking-normal text-foreground dark:text-white',
              cell.emphasis ? 'text-[25px] md:text-[30px] lg:text-[34px]' : 'text-[22px] md:text-[26px] lg:text-[30px]',
            )}
          >
            {cell.value}
          </div>
          {cell.sub && (
            <div
              className={cn(
                'mt-2 text-[10px] font-black uppercase tracking-[0.14em] lg:text-[11px]',
                cell.emphasis ? 'text-[#BE123C]/72 dark:text-[#FDA4AF]/70' : 'text-foreground/48 dark:text-white/44',
              )}
            >
              {cell.sub}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}

function ContentSection({
  label,
  children,
  tone = 'default',
  className,
}: {
  label: string;
  children: React.ReactNode;
  tone?: 'default' | 'accent';
  className?: string;
}) {
  return (
    <section
      className={cn(
        'min-w-0 max-w-full overflow-hidden rounded-[20px] border px-4 py-4 lg:px-5 lg:py-5',
        tone === 'accent'
          ? 'border-[#E11D48]/24 bg-[linear-gradient(135deg,rgba(225,29,72,0.13),rgba(225,29,72,0.04))]'
          : 'border-foreground/[0.07] bg-white/32 dark:border-white/[0.07] dark:bg-white/[0.025]',
        className,
      )}
    >
      <div
        className={cn(
          'text-[8px] font-black uppercase tracking-[0.18em] md:text-[9px]',
          tone === 'accent' ? 'text-[#E11D48] dark:text-[#FDA4AF]' : 'text-foreground/38 dark:text-white/32',
        )}
      >
        {label}
      </div>
      <div className="mt-3 min-w-0 text-[15.5px] font-bold leading-[1.48] text-foreground/78 dark:text-white/80 md:text-[16.5px] lg:text-[17.5px] lg:leading-[1.5]">
        {children}
      </div>
    </section>
  );
}

function PoolDialog({
  block,
  account,
  onClose,
}: {
  block: FeederPoolBlock;
  account: FeederPoolAccount;
  onClose: () => void;
}) {
  const dialogRef = useRef<HTMLDivElement | null>(null);
  const detailScrollRef = useRef<HTMLDivElement | null>(null);
  const members = useMemo(() => block.members || [], [block.members]);
  const initialKey = leaderMember(block)?.post_key || members[0]?.post_key || '';
  const [selectedKey, setSelectedKey] = useState<string>(initialKey);
  const selected = useMemo(
    () => members.find((member) => member.post_key === selectedKey) || members[0] || null,
    [members, selectedKey],
  );

  useEffect(() => {
    if (typeof window === 'undefined') return undefined;
    const previousOverflow = document.documentElement.style.overflow;
    document.documentElement.style.overflow = 'hidden';
    const onKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => {
      document.documentElement.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKey);
    };
  }, [onClose]);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      dialogRef.current?.scrollTo({ top: 0, behavior: 'instant' });
      detailScrollRef.current?.scrollTo({ top: 0, behavior: 'instant' });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [block.pool_id, block.pool_slug]);

  const popupCopy = block.frontend_copy?.popup;
  const coverCopy = block.frontend_copy?.cover;
  const description = popupCopy?.opening_read
    ? [popupCopy.opening_read]
    : block.description_sentences && block.description_sentences.length > 0
      ? block.description_sentences
      : [block.why_this_pool_exists || ''].filter(Boolean);

  // Show only the first description sentence as the punchy "what works" line.
  // Remaining sentences live under Source Detail.
  const leadDescription = description[0] || '';
  const extraDescription = description.slice(1);
  const belongBullets = popupCopy?.why_the_posts_belong_together || [];
  const performanceRead = popupCopy?.performance_read || '';
  const implementationRead = popupCopy?.implementation_read || '';

  const scanReason =
    selected?.frontend_copy?.selected_proof_line ||
    selected?.frontend_detail_packaging?.scan_layer ||
    selected?.yaml_verbatim?.why_it_clusters_here ||
    '';

  const keyMoments = selected?.frontend_detail_packaging?.evidence_layer?.key_moments || [];
  const receipts =
    selected?.frontend_copy?.receipt_chips ||
    selected?.frontend_detail_packaging?.evidence_layer?.raw ||
    selected?.yaml_verbatim?.receipts ||
    [];
  const reasoning = selected?.backend_intelligence?.expanded_pool_reasoning || '';
  const fullSummary =
    selected?.frontend_detail_packaging?.depth_layer?.expanded_backend_summary ||
    selected?.backend_intelligence?.expanded_summary ||
    selected?.yaml_verbatim?.summary ||
    '';
  const packagedSequence = sequenceFromCards(selected?.frontend_copy?.reasoning_cards);
  const sequence = packagedSequence.length > 0 ? packagedSequence : extractSequence(fullSummary);
  const selectedMetricLine = memberMetricLine(selected);

  return (
    <motion.div
      className="fixed inset-0 z-[320] flex items-end justify-center px-0 sm:items-center sm:px-3 sm:py-3 md:px-4 md:py-4 lg:px-5 lg:py-5"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.2, ease: GRID_ITEM_EASE }}
    >
      <motion.div
        className="absolute inset-0 bg-black/72 backdrop-blur-[8px]"
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        exit={{ opacity: 0 }}
        transition={{ duration: 0.2 }}
        onClick={onClose}
      />

      <motion.div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-label={block.headline}
        layout
        layoutId={blockLayoutId(block)}
        initial={{ opacity: 0, scale: 0.96, filter: 'blur(10px)' }}
        animate={{ opacity: 1, scale: 1, filter: 'blur(0px)' }}
        exit={{ opacity: 0, scale: 0.96, filter: 'blur(10px)' }}
        transition={{
          layout: GRID_LAYOUT_SPRING,
          opacity: { duration: 0.18, ease: GRID_ITEM_EASE },
          scale: { type: 'spring', stiffness: 300, damping: 32, mass: 0.85 },
          filter: { duration: 0.2, ease: GRID_ITEM_EASE },
        }}
        onClick={(event) => event.stopPropagation()}
        className="hide-scrollbar relative flex max-h-[92dvh] w-full flex-col overflow-y-auto overflow-x-hidden rounded-t-[28px] border border-foreground/[0.08] bg-[#f7f7f6] text-foreground shadow-[0_-18px_60px_rgba(15,23,42,0.18)] dark:border-white/[0.08] dark:bg-[#080808] dark:text-white dark:shadow-[0_-18px_60px_rgba(0,0,0,0.55)] sm:grid sm:h-[var(--pool-dialog-height)] sm:w-[min(1120px,calc(100vw-24px))] sm:grid-cols-[minmax(220px,0.74fr)_minmax(0,1.26fr)] sm:overflow-hidden sm:rounded-[24px] sm:shadow-[0_32px_90px_rgba(15,23,42,0.2)] sm:dark:shadow-[0_32px_90px_rgba(0,0,0,0.72)] md:w-[min(1180px,calc(100vw-40px))] md:grid-cols-[minmax(280px,0.72fr)_minmax(0,1.28fr)] md:rounded-[28px] lg:w-[min(1560px,calc(100vw-48px))] lg:grid-cols-[minmax(380px,470px)_minmax(0,1fr)] xl:w-[min(1680px,calc(100vw-64px))] xl:grid-cols-[minmax(420px,500px)_minmax(0,1fr)]"
        style={
          {
            maxHeight: 'min(880px, calc(100dvh - 44px - env(safe-area-inset-top)))',
            '--pool-dialog-height': 'min(880px, calc(100dvh - 44px - env(safe-area-inset-top)))',
          } as CSSProperties
        }
      >
        <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-[#E11D48]/42 to-transparent" />
        <div className="mx-auto mt-3 h-1.5 w-12 rounded-full bg-foreground/14 dark:bg-white/14 md:hidden" />

        <button
          type="button"
          onClick={onClose}
          className="absolute right-4 top-4 z-20 flex h-9 w-9 items-center justify-center rounded-full border border-foreground/[0.08] bg-foreground/[0.04] text-foreground/54 transition hover:bg-foreground/[0.08] hover:text-foreground/82 dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/54 dark:hover:bg-white/[0.1] dark:hover:text-white/82 md:right-5 md:top-5"
          aria-label="Close pool detail"
        >
          <X size={16} strokeWidth={2.4} />
        </button>

        <div className="order-1 px-5 pb-3 pt-12 sm:hidden">
          <div className="flex flex-wrap items-center gap-2">
            <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]/78">
              Feed_Me read
            </span>
            <span className="rounded-full border border-foreground/[0.08] bg-foreground/[0.04] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-foreground/58 dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/58">
              {humanSignal(block)}
            </span>
          </div>
          <h3 className="mt-3 max-w-[calc(100%-24px)] text-[36px] font-black leading-[0.93] tracking-normal text-foreground [text-wrap:balance] dark:text-white">
            {coverCopy?.headline_full || block.headline}
          </h3>
          {leadDescription && (
            <p className="mt-4 max-w-[calc(100%-8px)] text-[15px] font-bold leading-[1.42] text-foreground/72 [text-wrap:balance] dark:text-white/76">
              {leadDescription}
            </p>
          )}
        </div>

        {/* LEFT — featured + rail */}
        <div className="order-2 flex shrink-0 flex-col gap-3 px-5 pb-4 pt-2 sm:order-none sm:min-h-0 sm:justify-center sm:p-4 md:p-5 lg:gap-4 xl:p-6">
          <div className="flex min-h-0 items-center justify-center">
            <div className="aspect-[4/5] w-[min(78vw,318px)] max-w-full sm:w-full sm:max-w-[292px] md:max-w-[330px] lg:max-w-[420px] xl:max-w-[455px]">
              <FeaturedThumbnail member={selected} />
            </div>
          </div>
          <AnimatePresence mode="popLayout" initial={false}>
            {selected && (
              <motion.div
                key={`proof-dock-${selected.post_key}`}
                variants={POST_SWAP_ITEM_VARIANTS}
                initial="hidden"
                animate="show"
                exit="exit"
                layout
              >
                <SelectedProofDock
                  member={selected}
                  index={Math.max(1, members.findIndex((m) => m.post_key === selected.post_key) + 1)}
                  total={members.length}
                  mediaType={account.feeder.media_type}
                />
              </motion.div>
            )}
          </AnimatePresence>
          <MemberRail members={members} selectedKey={selected?.post_key || null} onSelect={setSelectedKey} />
        </div>

        {/* RIGHT — content */}
        <div className="order-3 relative min-w-0 max-w-full flex-1 sm:order-none sm:h-full sm:min-h-0 sm:overflow-hidden">
          <div
            ref={detailScrollRef}
            className="min-w-0 max-w-full px-4 pb-[calc(22px+env(safe-area-inset-bottom))] pt-3 sm:hide-scrollbar sm:h-full sm:min-h-0 sm:touch-pan-y sm:overflow-y-auto sm:overscroll-y-contain sm:px-5 sm:py-5 md:px-6 md:py-6 lg:px-7 lg:py-7 xl:px-8 xl:py-8"
          >
            {/* Pattern briefing */}
            <div className="hidden shrink-0 pr-10 sm:block md:pr-12">
              <div className="rounded-[24px] border border-foreground/[0.07] bg-white/36 p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.3)] backdrop-blur-[18px] dark:border-white/[0.07] dark:bg-white/[0.03] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.055)] md:p-5 lg:p-6">
                <div className="flex flex-wrap items-center gap-2.5">
                  <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]/72 xl:text-[10px]">
                    Feed_Me read
                  </span>
                  <span className="rounded-full border border-foreground/[0.08] bg-foreground/[0.04] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-foreground/56 dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/56">
                    {humanSignal(block)}
                  </span>
                  {block.kind === 'candidate' && (
                    <span className="rounded-full border border-foreground/[0.08] bg-foreground/[0.04] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-foreground/56 dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/56">
                      Candidate
                    </span>
                  )}
                </div>
                <h3 className="mt-3 max-w-[calc(100%-10px)] text-[30px] font-black leading-[0.98] tracking-normal text-foreground [text-wrap:balance] dark:text-white sm:max-w-[min(760px,calc(100%-56px))] sm:text-[33px] md:mt-4 md:max-w-[min(920px,calc(100%-64px))] md:text-[39px] lg:text-[46px] lg:leading-[0.96] xl:max-w-[min(1040px,calc(100%-70px))] xl:text-[52px]">
                  {coverCopy?.headline_full || block.headline}
                </h3>
                <div className="mt-5 grid gap-3 lg:grid-cols-[minmax(0,1.1fr)_minmax(340px,0.9fr)] lg:items-stretch">
                  {leadDescription && (
                    <div className="rounded-[20px] border border-[#E11D48]/20 bg-[linear-gradient(135deg,rgba(225,29,72,0.12),rgba(225,29,72,0.04))] px-4 py-4 md:px-5 md:py-5">
                      <div className="text-[9px] font-black uppercase tracking-[0.18em] text-[#E11D48] dark:text-[#FDA4AF]">
                        Pool read
                      </div>
                      <p className="mt-2.5 text-[18px] font-black leading-[1.32] text-foreground/84 [text-wrap:balance] dark:text-white/86 md:text-[19px] lg:text-[21px]">
                        {leadDescription}
                      </p>
                    </div>
                  )}
                  <div className={cn(!leadDescription && 'lg:col-span-2')}>
                    <StatRow block={block} />
                  </div>
                </div>
              </div>
            </div>

            {/* Member-dependent sections — crossfade on selection change */}
            <AnimatePresence mode="popLayout" initial={false}>
              <motion.div
                key={selected?.post_key || 'empty-post'}
                className="mt-4 grid min-w-0 max-w-full gap-3 lg:gap-3.5 2xl:grid-cols-[minmax(0,1fr)_minmax(380px,0.78fr)] [&>*]:min-w-0 [&>*]:max-w-full"
                variants={POST_SWAP_CONTAINER_VARIANTS}
                initial="hidden"
                animate="show"
                exit="exit"
                layout
              >
                {scanReason && (
                  <motion.div variants={POST_SWAP_ITEM_VARIANTS} className="min-w-0 max-w-full 2xl:col-span-2" layout>
                    <ContentSection label="Selected proof" tone="accent">
                      {selectedMetricLine && (
                        <motion.div
                          variants={POST_SWAP_TILE_VARIANTS}
                        className="mb-4 inline-flex max-w-full rounded-full border border-[#E11D48]/20 bg-white/48 px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-[#BE123C] shadow-[inset_0_1px_0_rgba(255,255,255,0.2)] dark:bg-black/18 dark:text-[#FDA4AF] md:text-[11px]"
                        >
                          <span className="truncate">{selectedMetricLine}</span>
                        </motion.div>
                      )}
                      <motion.div
                        variants={POST_SWAP_TILE_VARIANTS}
                        className="max-w-[1100px] text-[20px] font-black leading-[1.26] text-foreground dark:text-white md:text-[24px] lg:text-[27px] [text-wrap:balance]"
                      >
                        {scanReason}
                      </motion.div>
                    </ContentSection>
                  </motion.div>
                )}

                {(belongBullets.length > 0 || performanceRead || implementationRead) && (
                  <motion.div variants={POST_SWAP_ITEM_VARIANTS} className="min-w-0 max-w-full 2xl:col-span-2" layout>
                    <ContentSection label="Pattern read">
                      <div className="grid gap-3 lg:grid-cols-[minmax(0,0.95fr)_minmax(0,1.05fr)]">
                        {belongBullets.length > 0 && (
                          <div className="rounded-[16px] border border-foreground/[0.06] bg-white/34 px-3.5 py-3.5 dark:border-white/[0.06] dark:bg-black/20 lg:px-4 lg:py-4">
                            <div className="text-[9px] font-black uppercase tracking-[0.16em] text-[#FB7185]/72">
                              Why these sit together
                            </div>
                            <ul className="mt-3 space-y-2.5 text-[14.5px] font-black leading-[1.34] text-foreground/74 dark:text-white/76 md:text-[16px]">
                              {belongBullets.map((bullet, idx) => (
                                <li key={idx} className="flex gap-2.5">
                                  <span className="mt-2 inline-block h-1.5 w-1.5 shrink-0 rounded-full bg-[#E11D48]/62" />
                                  <span>{bullet}</span>
                                </li>
                              ))}
                            </ul>
                          </div>
                        )}
                        <div className="grid gap-3">
                          {performanceRead && (
                            <div className="rounded-[16px] border border-foreground/[0.06] bg-white/34 px-3.5 py-3.5 dark:border-white/[0.06] dark:bg-black/20 lg:px-4 lg:py-4">
                              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-[#FB7185]/72">
                                Why it has weight
                              </div>
                              <p className="mt-2.5 text-[15px] font-black leading-[1.38] text-foreground/74 dark:text-white/76 md:text-[16.5px]">
                                {performanceRead}
                              </p>
                            </div>
                          )}
                          {implementationRead && (
                            <div className="rounded-[16px] border border-foreground/[0.06] bg-white/34 px-3.5 py-3.5 dark:border-white/[0.06] dark:bg-black/20 lg:px-4 lg:py-4">
                              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-[#FB7185]/72">
                                How to adapt it
                              </div>
                              <p className="mt-2.5 text-[15px] font-black leading-[1.38] text-foreground/74 dark:text-white/76 md:text-[16.5px]">
                                {implementationRead}
                              </p>
                            </div>
                          )}
                        </div>
                      </div>
                    </ContentSection>
                  </motion.div>
                )}

                {sequence.length > 0 && (
                  <motion.div variants={POST_SWAP_ITEM_VARIANTS} className="min-w-0 max-w-full" layout>
                    <ContentSection label="Repeatable sequence">
                    <motion.div
                      variants={{
                        show: { transition: { staggerChildren: 0.045, delayChildren: 0.04 } },
                      }}
                      className="grid gap-2.5 min-[620px]:grid-cols-2"
                    >
                      {sequence.map((item) => (
                        <motion.div
                          key={item.label}
                          variants={POST_SWAP_TILE_VARIANTS}
                          layout
                          className="min-h-0 rounded-[16px] border border-foreground/[0.06] bg-white/36 px-3 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.22)] dark:border-white/[0.06] dark:bg-black/24 dark:shadow-none sm:min-h-[124px] lg:px-4 lg:py-4"
                        >
                          <div className="text-[9px] font-black uppercase tracking-[0.16em] text-[#FB7185]/72">
                            {item.label}
                          </div>
                          <div className="mt-2 text-[14.5px] font-black leading-[1.34] text-foreground/78 dark:text-white/80 md:text-[16.5px]">
                            {item.text}
                          </div>
                        </motion.div>
                      ))}
                    </motion.div>
                    </ContentSection>
                  </motion.div>
                )}

                {(keyMoments.length > 0 || receipts.length > 0) && (
                  <motion.div
                    variants={POST_SWAP_ITEM_VARIANTS}
                    className={cn('min-w-0 max-w-full', !sequence.length && '2xl:col-span-2')}
                    layout
                  >
                  <ContentSection label="Receipts">
                    {keyMoments.length > 0 && (
                      <motion.div
                        variants={{
                          show: { transition: { staggerChildren: 0.035, delayChildren: 0.03 } },
                        }}
                        className="mb-3 flex flex-wrap gap-1.5"
                      >
                        {keyMoments.map((moment, idx) => (
                          <motion.span
                            key={`${moment.timestamp}-${idx}`}
                            variants={POST_SWAP_TILE_VARIANTS}
                            className="inline-flex items-center gap-1.5 rounded-full border border-foreground/[0.08] bg-foreground/[0.035] px-3 py-1.5 text-[11px] font-black tracking-normal text-foreground/72 dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/72"
                          >
                            <span className="text-[#FB7185]">{moment.timestamp}</span>
                            <span className="text-foreground/56 dark:text-white/56">{moment.label}</span>
                          </motion.span>
                        ))}
                      </motion.div>
                    )}
                    {receipts.length > 0 && (
                      <ul className="space-y-2.5 text-[15px] font-bold leading-[1.5] text-foreground/70 dark:text-white/72 md:text-[16px]">
                        {receipts.map((receipt, idx) => (
                          <motion.li key={idx} variants={POST_SWAP_TILE_VARIANTS} className="flex gap-2.5">
                            <span className="mt-2 inline-block h-1.5 w-1.5 shrink-0 rounded-full bg-[#E11D48]/58 dark:bg-[#FB7185]/58" />
                            <span>{receipt}</span>
                          </motion.li>
                        ))}
                      </ul>
                    )}
                  </ContentSection>
                  </motion.div>
                )}

                {(extraDescription.length > 0 || fullSummary || reasoning) && (
                  <motion.details
                    variants={POST_SWAP_ITEM_VARIANTS}
                    layout
                    className="min-w-0 max-w-full rounded-[16px] border border-foreground/[0.06] bg-foreground/[0.02] px-4 py-3.5 dark:border-white/[0.06] dark:bg-white/[0.02] 2xl:col-span-2 lg:px-5 lg:py-4"
                  >
                    <summary className="cursor-pointer text-[9px] font-black uppercase tracking-[0.18em] text-foreground/42 dark:text-white/40">
                      Source detail
                    </summary>
                    <div className="mt-3 space-y-4 text-[12.5px] font-semibold leading-relaxed text-foreground/60 dark:text-white/60 lg:text-[13.5px] lg:leading-[1.6]">
                      {extraDescription.length > 0 && (
                        <div className="space-y-2">
                          {extraDescription.map((sentence, idx) => (
                            <p key={idx}>{sentence}</p>
                          ))}
                        </div>
                      )}
                      {fullSummary && (
                        <div className="whitespace-pre-line border-t border-foreground/[0.06] pt-4 dark:border-white/[0.06]">{fullSummary}</div>
                      )}
                      {reasoning && (
                        <div className="whitespace-pre-line border-t border-foreground/[0.06] pt-4 dark:border-white/[0.06]">{reasoning}</div>
                      )}
                    </div>
                  </motion.details>
                )}
              </motion.div>
            </AnimatePresence>
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
}

// ── main hero ───────────────────────────────────────────────────────────────

function useFeederPoolPayload() {
  const [payload, setPayload] = useState<FeederPoolPayload | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    fetch('/api/feed/feeder-pool-sample')
      .then(async (response) => {
        const json = await response.json();
        if (!response.ok) throw new Error(json.error || 'Unable to load pool packet');
        return json as FeederPoolPayload;
      })
      .then((next) => {
        if (cancelled) return;
        setPayload(next);
        setError(next.error || null);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : 'Unable to load pool packet');
        setPayload(null);
      })
      .finally(() => {
        if (!cancelled) setIsLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  return { payload, isLoading, error };
}

export default function FeederPoolHero({ selectedHandle = 'all' }: FeederPoolHeroProps) {
  const { payload, isLoading, error } = useFeederPoolPayload();
  const [manualHandle, setManualHandle] = useState<string>('');
  const [selectedBlock, setSelectedBlock] = useState<FeederPoolBlock | null>(null);
  const [mounted, setMounted] = useState(false);

  const accounts = useMemo(() => {
    const next = payload?.accounts || [];
    return [...next].sort((a, b) => {
      const ai = ACCOUNT_ORDER.indexOf(normalizeHandle(a.feeder.handle));
      const bi = ACCOUNT_ORDER.indexOf(normalizeHandle(b.feeder.handle));
      return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
    });
  }, [payload]);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => setMounted(true));
    return () => window.cancelAnimationFrame(frame);
  }, []);

  const handleSelectAccount = useCallback((handle: string) => {
    setManualHandle(handle);
    setSelectedBlock(null);
  }, []);

  const selectedNormalized = normalizeHandle(selectedHandle);
  const selectedExists = accounts.some((a) => normalizeHandle(a.feeder.handle) === selectedNormalized);
  const manualExists = accounts.some((a) => normalizeHandle(a.feeder.handle) === manualHandle);
  const activeHandle =
    selectedExists && selectedNormalized !== 'all'
      ? selectedNormalized
      : manualExists
        ? manualHandle
        : normalizeHandle(accounts[0]?.feeder.handle);

  const account = accounts.find((a) => normalizeHandle(a.feeder.handle) === activeHandle) || accounts[0] || null;
  const pools = account ? sortedBlocks(account.blocks || []) : [];
  const candidates = account ? sortedBlocks(account.candidate_blocks || []) : [];
  const accountKey = normalizeHandle(account?.feeder.handle);

  if (isLoading) {
    return (
      <section className="fm-depth-glass rounded-[24px] p-4 sm:p-5">
        <div className="h-4 w-44 rounded-full bg-foreground/8 dark:bg-white/8" />
        <div className="mt-4 grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-6">
          {[0, 1, 2, 3].map((i) => (
            <div
              key={i}
              className="min-h-[380px] rounded-[22px] border border-black/5 bg-foreground/[0.035] dark:border-white/8 dark:bg-white/[0.04] sm:col-span-1 lg:col-span-3"
            />
          ))}
        </div>
      </section>
    );
  }

  if (!account || error) {
    return (
      <section className="fm-depth-glass rounded-[24px] p-5">
        <div className="text-[10px] font-black uppercase tracking-[0.18em] text-foreground/42 dark:text-white/34">
          Feeder pools unavailable
        </div>
        <div className="mt-2 text-[13px] font-semibold text-foreground/54 dark:text-white/44">
          {error || 'No pool packet was returned.'}
        </div>
      </section>
    );
  }

  return (
    <LayoutGroup id="feeder-pool-dashboard">
      <section className="fm-depth-glass relative -mx-3 rounded-[22px] p-1.5 pb-[calc(104px+env(safe-area-inset-bottom))] sm:mx-0 sm:rounded-[26px] sm:p-4">
      <div className="mb-2.5 flex flex-col gap-3 px-0.5 sm:mb-3 sm:flex-row sm:items-end sm:justify-between sm:px-0">
        <div>
          <div className="flex items-center gap-2">
            <span className="h-2 w-2 rounded-full bg-[#E11D48] shadow-[0_0_12px_rgba(225,29,72,0.6)]" />
            <div className="text-[9px] font-black uppercase tracking-[0.18em] text-foreground/42 dark:text-white/34">
              {account.header.state_label}
            </div>
          </div>
          <h2 className="fm-depth-title mt-1 text-[26px] font-black leading-none tracking-normal text-foreground dark:text-white sm:text-[32px] lg:text-[36px]">
            {account.feeder.display_handle}
          </h2>
          <div className="mt-1.5 text-[11px] font-black tracking-normal text-foreground/48 dark:text-white/42">
            {account.header.meta_line}
          </div>
          <div className="mt-2 inline-flex rounded-full border border-foreground/[0.08] bg-foreground/[0.035] px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.15em] text-foreground/54 dark:border-white/[0.08] dark:bg-white/[0.045] dark:text-white/48 sm:hidden">
            Showing {pools.length} patterns · {candidates.length} candidates
          </div>
        </div>

        <div className="hide-scrollbar -mx-1 flex max-w-full gap-1 overflow-x-auto rounded-[18px] border border-foreground/[0.07] bg-white/48 p-1 shadow-[inset_0_1px_0_rgba(255,255,255,0.42),0_12px_26px_-24px_rgba(15,23,42,0.5)] backdrop-blur-[18px] dark:border-white/[0.08] dark:bg-white/[0.04] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.055),0_12px_26px_-24px_rgba(0,0,0,0.9)] sm:mx-0 sm:justify-end">
          {accounts.map((entry) => {
            const handle = normalizeHandle(entry.feeder.handle);
            const isActive = handle === normalizeHandle(account.feeder.handle);
            return (
              <motion.button
                key={handle}
                layout
                type="button"
                onClick={() => handleSelectAccount(handle)}
                className={cn(
                  'relative isolate shrink-0 overflow-hidden rounded-[14px] px-2.5 py-1.5 text-[8.5px] font-black uppercase tracking-[0.12em] transition min-[430px]:px-3 min-[430px]:py-2 sm:px-3.5 sm:py-2 sm:text-[9px] lg:px-4 lg:text-[10px]',
                  isActive
                    ? 'text-white'
                    : 'text-foreground/46 hover:bg-foreground/[0.035] hover:text-foreground/72 dark:text-white/42 dark:hover:bg-white/[0.045] dark:hover:text-white/72',
                )}
              >
                {isActive && (
                  <motion.span
                    layoutId="feeder-picker-active-pill"
                    className="absolute inset-0 z-0 rounded-[14px] bg-[#E11D48] shadow-[0_12px_22px_-16px_rgba(225,29,72,0.95)]"
                    transition={{ type: 'spring', stiffness: 420, damping: 34, mass: 0.78 }}
                  >
                    <motion.span
                      className="absolute inset-0 rounded-[inherit] ring-1 ring-white/28"
                      animate={{ opacity: [0.16, 0.3, 0.16], scale: [1, 1.012, 1] }}
                      transition={{ duration: 2.8, repeat: Infinity, ease: 'easeInOut' }}
                    />
                  </motion.span>
                )}
                <span className="relative z-10 inline-flex items-center gap-1.5">
                  <span
                    className={cn(
                      'h-1 w-1 rounded-full transition sm:h-1.5 sm:w-1.5',
                      isActive ? 'bg-white shadow-[0_0_10px_rgba(255,255,255,0.55)]' : 'bg-foreground/18 dark:bg-white/18',
                    )}
                  />
                  @{handle}
                </span>
              </motion.button>
            );
          })}
        </div>
      </div>

      <AnimatePresence mode="popLayout" initial={false}>
        <motion.div
          key={accountKey}
          initial={{ opacity: 0, y: 18, scale: 0.985, filter: 'blur(8px)' }}
          animate={{ opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
          exit={{ opacity: 0, y: -14, scale: 0.985, filter: 'blur(5px)' }}
          transition={{
            opacity: { duration: 0.24, ease: GRID_ITEM_EASE },
            y: { type: 'spring', stiffness: 320, damping: 30, mass: 0.82 },
            scale: { duration: 0.28, ease: GRID_ITEM_EASE },
            filter: { duration: 0.22, ease: GRID_ITEM_EASE },
          }}
          layout
        >
          {/* Uniform-size pool tiles. 2 per row on desktop, 1 per row on mobile. */}
          <motion.div
            layout
            transition={{ layout: GRID_LAYOUT_SPRING }}
            className="grid grid-cols-1 gap-2.5 sm:grid-cols-2 sm:gap-3 lg:grid-cols-6"
          >
            {pools.map((block, index) => (
              <PoolTile key={block.pool_id} block={block} index={index} onOpen={setSelectedBlock} />
            ))}
          </motion.div>

          {candidates.length > 0 && (
            <motion.div
              initial={{ opacity: 0, y: 18, scale: 0.975, filter: 'blur(6px)' }}
              animate={{ opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
              exit={{ opacity: 0, y: -8, scale: 0.975, filter: 'blur(4px)' }}
              transition={{
                opacity: { duration: 0.24, delay: Math.min(pools.length * 0.035, 0.18), ease: GRID_ITEM_EASE },
                y: { type: 'spring', stiffness: 330, damping: 30, mass: 0.8, delay: Math.min(pools.length * 0.035, 0.18) },
                scale: { duration: 0.26, delay: Math.min(pools.length * 0.035, 0.18), ease: GRID_ITEM_EASE },
                filter: { duration: 0.22, delay: Math.min(pools.length * 0.035, 0.18), ease: GRID_ITEM_EASE },
              }}
              className="mt-5"
            >
              <div className="mb-2 flex items-baseline justify-between gap-3">
                <div>
                  <div className="text-[9px] font-black uppercase tracking-[0.18em] text-foreground/42 dark:text-white/34">
                    Candidates
                  </div>
                  <div className="text-[12px] font-black tracking-normal text-foreground/64 dark:text-white/52">
                    Patterns watching for company
                  </div>
                </div>
                <span className="rounded-full border border-black/6 bg-white/48 px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.12em] text-foreground/48 dark:border-white/8 dark:bg-white/[0.05] dark:text-white/40">
                  {candidates.length}
                </span>
              </div>
              <motion.div
                layout
                transition={{ layout: GRID_LAYOUT_SPRING }}
                className="hide-scrollbar -mx-1 flex gap-2 overflow-x-auto px-1 pb-1"
              >
                {candidates.map((block, index) => (
                  <CandidateChip
                    key={block.pool_id}
                    block={block}
                    index={pools.length + index + 1}
                    onOpen={setSelectedBlock}
                  />
                ))}
              </motion.div>
            </motion.div>
          )}
        </motion.div>
      </AnimatePresence>

      {mounted &&
        createPortal(
          <AnimatePresence>
            {selectedBlock && account && (
              <PoolDialog
                key={selectedBlock.pool_slug || selectedBlock.pool_id}
                block={selectedBlock}
                account={account}
                onClose={() => setSelectedBlock(null)}
              />
            )}
          </AnimatePresence>,
          document.body,
        )}
      </section>
    </LayoutGroup>
  );
}
