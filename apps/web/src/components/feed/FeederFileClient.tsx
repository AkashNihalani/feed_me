'use client';

import { type CSSProperties, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, ArrowUpRight, BrainCircuit, ChevronRight, X } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import type { FeederFilePattern, MetricCard, ProofBlock } from '@/types/feederFile';

/* ─── types ─── */

type FeederFileClientProps = {
  feedId: string;
  selectedHandle?: string;
};

/* ─── thumbnail cache ─── */

const THUMBNAIL_FAILURE_TTL_MS = 10 * 60 * 1000;
const thumbnailFailureCache = new Map<string, number>();

function mediaProxyUrl(postKey: string | null | undefined): string {
  const key = (postKey || '').trim();
  return key ? `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail` : '';
}

function instagramPostUrl(postKey: string | null | undefined, postUrl?: string | null): string {
  const explicitUrl = String(postUrl || '').trim();
  if (explicitUrl) return explicitUrl;
  const cleanKey = String(postKey || '').trim().split('#')[0];
  const parts = cleanKey.split('/').filter(Boolean);
  const shortcode = parts.at(-1);
  const mediaType = parts[0] === 'reel' ? 'reel' : 'p';
  return shortcode ? `https://www.instagram.com/${mediaType}/${encodeURIComponent(shortcode)}/` : 'https://www.instagram.com/';
}

function isThumbnailFailureCached(postKey: string | null | undefined): boolean {
  const key = (postKey || '').trim();
  if (!key) return false;
  const failedAt = thumbnailFailureCache.get(key);
  if (!failedAt) return false;
  if (Date.now() - failedAt > THUMBNAIL_FAILURE_TTL_MS) {
    thumbnailFailureCache.delete(key);
    return false;
  }
  return true;
}

function rememberThumbnailFailure(postKey: string | null | undefined) {
  const key = (postKey || '').trim();
  if (key) thumbnailFailureCache.set(key, Date.now());
}

/* ─── constants ─── */

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const ACCENT = '#E11D48';
const DEFAULT_ACCOUNT = '';
const STORY_RING_RADIUS = 44;
const STORY_RING_CIRCUMFERENCE = 2 * Math.PI * STORY_RING_RADIUS;

const ACCOUNT_INITIALS: Record<string, string> = {
  '@anuj.mp4': 'AJ',
  '@lakmeindia': 'LK',
};

const EMPTY_PROOF: ProofBlock = {
  post_key: '',
  post_url: null,
  proof_label: '',
  proof_headline: '',
  post_read: '',
  what_clicked: '',
  evidence: [],
  metrics: [],
};

type FeedAvatarMap = Partial<Record<string, string>>;

function accountForHandle(raw: string | null | undefined): string | null {
  const handle = String(raw || '').trim().replace(/^@+/, '').toLowerCase();
  return handle && handle !== 'all' ? `@${handle}` : null;
}

function patternsForAccount(selectedAccount: string, patterns: FeederFilePattern[]): FeederFilePattern[] {
  const handle = String(selectedAccount || '').replace(/^@+/, '').toLowerCase();
  if (!handle || handle === 'all') return patterns;
  return patterns.filter((pattern) => pattern.account.replace(/^@+/, '').toLowerCase() === handle);
}

/* ─── corner ticks ─── */

function CornerTicks({ color = ACCENT, size = 12, inset = 8 }: { color?: string; size?: number; inset?: number }) {
  const s = `${size}px`;
  const shared: CSSProperties = { position: 'absolute', width: s, height: s, borderColor: color, pointerEvents: 'none' };
  return (
    <>
      <span style={{ ...shared, top: inset, left: inset, borderTop: `2px solid ${color}`, borderLeft: `2px solid ${color}` }} />
      <span style={{ ...shared, top: inset, right: inset, borderTop: `2px solid ${color}`, borderRight: `2px solid ${color}` }} />
      <span style={{ ...shared, bottom: inset, left: inset, borderBottom: `2px solid ${color}`, borderLeft: `2px solid ${color}` }} />
      <span style={{ ...shared, bottom: inset, right: inset, borderBottom: `2px solid ${color}`, borderRight: `2px solid ${color}` }} />
    </>
  );
}

/* ─── thumb (proof thumbnail) ─── */

function Thumb({
  post,
  className = '',
  index,
  total,
  showCaption = false,
  onUnavailable,
  fit = 'cover',
}: {
  post: ProofBlock;
  className?: string;
  index?: number;
  total?: number;
  showCaption?: boolean;
  onUnavailable?: (postKey: string) => void;
  fit?: 'cover' | 'contain';
}) {
  const fallback = mediaProxyUrl(post.post_key);
  const [failedPostKeys, setFailedPostKeys] = useState<Set<string>>(() => new Set());
  const dead = failedPostKeys.has(post.post_key) || isThumbnailFailureCached(post.post_key);
  const microMetric = showCaption ? post.metrics.find((m) => !m.accent) || post.metrics[0] : null;

  return (
    <div className={['relative overflow-hidden bg-[#f5f0f2] dark:bg-white/[0.06]', className].join(' ')}>
      {!dead && fallback ? (
        // eslint-disable-next-line @next/next/no-img-element -- dynamic post thumbnails are served through the media proxy
        <img
          src={fallback}
          alt=""
          className={[
            'h-full w-full transition-transform duration-700 ease-out',
            fit === 'contain' ? 'object-contain' : 'object-cover group-hover/card:scale-[1.035]',
          ].join(' ')}
          loading="lazy"
          decoding="async"
          onError={() => {
            rememberThumbnailFailure(post.post_key);
            onUnavailable?.(post.post_key);
            setFailedPostKeys((current) => {
              if (current.has(post.post_key)) return current;
              const next = new Set(current);
              next.add(post.post_key);
              return next;
            });
          }}
        />
      ) : (
        <div className="flex h-full w-full items-center justify-center bg-[radial-gradient(circle_at_28%_20%,rgba(225,29,72,0.16),transparent_42%),linear-gradient(135deg,#fff,#f9eef2)] px-5 text-center text-[11px] font-black uppercase tracking-[0.14em] text-[#E11D48]/42 dark:bg-[radial-gradient(circle_at_28%_20%,rgba(251,113,133,0.22),transparent_42%),linear-gradient(135deg,#171717,#09090b)] dark:text-[#FDA4AF]/54">
          {post.proof_label}
        </div>
      )}

      {showCaption && index !== undefined && total !== undefined && (
        <div className="pointer-events-none absolute right-2.5 top-2.5 z-10 flex h-6 items-center justify-center rounded-full bg-black/64 px-2 text-[9.5px] font-black tracking-wider text-white">
          {index}/{total}
        </div>
      )}

      {showCaption && (
        <div className="pointer-events-none absolute inset-x-0 bottom-0 flex items-end justify-between gap-2 bg-[linear-gradient(0deg,rgba(0,0,0,0.74),transparent_82%)] px-3 pb-3 pt-12">
          <div className="line-clamp-2 min-w-0 text-[9px] font-black uppercase leading-snug tracking-[0.16em] text-white">
            {index !== undefined && <span className="opacity-66">{String(index).padStart(2, '0')} / </span>}
            {post.proof_label}
          </div>
          {microMetric && (
            <div className="shrink-0 rounded-full bg-white/24 px-2 py-1 text-[9px] font-black uppercase tracking-[0.14em] text-white">
              {microMetric.value}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ─── metric pill (compact inline) ─── */

function readParagraphs(text: string, targetCount = 3): string[] {
  const cleaned = text.trim();
  if (!cleaned) return [];

  const sentences = cleaned
    .split(/(?<=[.!?])\s+(?=(?:"|[A-Z0-9]))/)
    .map((sentence) => sentence.trim())
    .filter(Boolean);

  if (sentences.length <= 1) return [cleaned];
  if (sentences.length <= targetCount) return sentences;

  const paragraphs: string[] = [];
  let cursor = 0;
  while (cursor < sentences.length && paragraphs.length < targetCount) {
    const remainingSentences = sentences.length - cursor;
    const remainingParagraphs = targetCount - paragraphs.length;
    const take = Math.ceil(remainingSentences / remainingParagraphs);
    paragraphs.push(sentences.slice(cursor, cursor + take).join(' '));
    cursor += take;
  }

  return paragraphs;
}

function metricByLabel(metrics: MetricCard[], label: string): MetricCard | undefined {
  return metrics.find((metric) => metric.label.toLowerCase() === label.toLowerCase());
}

function buildProofSignalCards(
  proof: ProofBlock,
  pattern: FeederFilePattern,
  proofIndex: number,
  total: number,
): MetricCard[] {
  const proofMetric = metricByLabel(proof.metrics, 'Proof');
  const bestRank = metricByLabel(proof.metrics, 'Best rank');
  const baseline = metricByLabel(proof.metrics, 'Baseline');
  const fallbackBaseline = pattern.proofs.flatMap((post) => post.metrics).find((metric) => metric.label === 'Baseline');
  const views = metricByLabel(proof.metrics, 'Views');
  const comments = metricByLabel(proof.metrics, 'Comments');
  const signal = metricByLabel(proof.metrics, 'Signal');
  const support = metricByLabel(proof.metrics, 'Support');
  const mode = metricByLabel(proof.metrics, 'Mode');
  const patternRank = pattern.patternMetrics.find((metric) => metric.accent) || pattern.patternMetrics[0];
  const patternSignal = metricByLabel(pattern.patternMetrics, 'Signal');

  return [
    {
      label: 'Proof role',
      value: proofMetric?.value || `${proofIndex + 1}/${total}`,
      detail: proofMetric?.detail || 'selected proof',
      accent: true,
    },
    {
      label: 'Performance',
      value: bestRank?.value || patternRank?.value || 'Pattern',
      detail: bestRank?.detail || patternRank?.detail || 'feeder context',
    },
    {
      label: 'Primary signal',
      value: signal?.value || patternSignal?.value || comments?.label || 'Read',
      detail: signal?.detail || patternSignal?.detail || comments?.detail || 'why it lifts',
    },
    {
      label: 'Pattern role',
      value: support?.value || 'Core',
      detail: support?.detail || mode?.detail || 'same pattern mechanic',
    },
    {
      label: 'Lift context',
      value: baseline?.value || fallbackBaseline?.value || 'Tracked',
      detail: baseline?.detail || fallbackBaseline?.detail || 'pattern lift proxy',
    },
    {
      label: 'Scale cue',
      value: views?.value || comments?.value || signal?.value || 'Qual',
      detail: views?.detail || comments?.detail || signal?.detail || 'selected proof',
    },
  ];
}

function hasRenderableProofs(pattern: FeederFilePattern): boolean {
  return Array.isArray(pattern.proofs) && pattern.proofs.some((proof) => Boolean(proof?.post_key));
}

/* ═══════════════════════════════════════════
   1. STORY RING STRIP
   ═══════════════════════════════════════════ */

function StoryRing({
  account,
  active,
  isAnchor,
  profilePicUrl,
  onSelect,
}: {
  account: string;
  active: boolean;
  isAnchor: boolean;
  profilePicUrl?: string | null;
  onSelect: () => void;
}) {
  const initials = ACCOUNT_INITIALS[account] || account.replace(/^@+/, '').slice(0, 2).toUpperCase();
  const [failedProfilePicUrl, setFailedProfilePicUrl] = useState<string | null>(null);
  const showProfilePic = Boolean(profilePicUrl) && failedProfilePicUrl !== profilePicUrl;

  return (
    <button
      type="button"
      onClick={onSelect}
      className="group flex shrink-0 flex-col items-center gap-1.5"
    >
      {/* ring + portrait */}
      <div className="relative">
        <div
          className="relative flex h-[78px] w-[78px] items-center justify-center rounded-full p-[5px] transition-all duration-300 sm:h-[88px] sm:w-[88px]"
        >
          <span
            className={[
              'absolute inset-0 rounded-full transition-colors duration-300',
              active
                ? 'bg-[#FFE4EA] shadow-[0_10px_26px_-18px_rgba(225,29,72,0.9)] dark:bg-[#3F0F1B]'
                : 'bg-black/[0.08] dark:bg-white/[0.12]',
            ].join(' ')}
          />
          {active && (
            <>
              <motion.span
                className="pointer-events-none absolute -inset-1 rounded-full bg-[#E11D48]/28 blur-md"
                initial={{ opacity: 0, scale: 0.86 }}
                animate={{ opacity: [0, 0.72, 0], scale: [0.86, 1.18, 1.02] }}
                transition={{ duration: 0.62, ease: APPLE_EASE, times: [0, 0.38, 1] }}
              />
              <motion.svg
                className="pointer-events-none absolute inset-0 z-10 h-full w-full overflow-visible"
                viewBox="0 0 100 100"
                aria-hidden="true"
              >
                <g transform="rotate(-90 50 50)">
                  <circle
                    cx="50"
                    cy="50"
                    r={STORY_RING_RADIUS}
                    fill="none"
                    stroke="rgba(225,29,72,0.24)"
                    strokeWidth="8"
                  />
                  <motion.circle
                    cx="50"
                    cy="50"
                    r={STORY_RING_RADIUS}
                    fill="none"
                    stroke={ACCENT}
                    strokeWidth="8"
                    strokeLinecap="round"
                    strokeDasharray={STORY_RING_CIRCUMFERENCE}
                    initial={{ opacity: 0.92, strokeDashoffset: STORY_RING_CIRCUMFERENCE }}
                    animate={{ opacity: 1, strokeDashoffset: 0 }}
                    transition={{ duration: 0.72, ease: APPLE_EASE }}
                    style={{ filter: 'drop-shadow(0 3px 8px rgba(225,29,72,0.42))' }}
                  />
                </g>
              </motion.svg>
            </>
          )}
          <div className="relative z-20 flex h-full w-full items-center justify-center overflow-hidden rounded-full border-[2px] border-white bg-[linear-gradient(135deg,#fce7f3,#fff1f2)] text-[22px] font-black text-[#9F1239] dark:border-[#09090b] dark:bg-[linear-gradient(135deg,#1c1917,#18181b)] dark:text-[#FDA4AF] sm:text-[25px]">
            {showProfilePic ? (
              // eslint-disable-next-line @next/next/no-img-element -- feeder avatars are proxied dynamic profile images
              <img
                src={profilePicUrl || ''}
                alt={account}
                className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-105"
                loading="lazy"
                decoding="async"
                onError={() => setFailedProfilePicUrl(profilePicUrl || null)}
              />
            ) : (
              initials
            )}
          </div>
        </div>

        {/* anchor dot */}
        {isAnchor && (
          <span className="absolute -bottom-0.5 left-1/2 h-3 w-3 -translate-x-1/2 rounded-full border-[2px] border-white bg-[#E11D48] dark:border-[#09090b]" />
        )}
      </div>

      {/* handle label */}
      <span
        className={[
          'text-[10px] font-black uppercase tracking-[0.12em] transition-colors duration-200',
          active ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black/38 dark:text-white/34',
        ].join(' ')}
        style={{ fontFamily: 'monospace' }}
      >
        {account}
      </span>
    </button>
  );
}

function StoryStrip({
  accounts,
  activeAccount,
  anchorAccount,
  avatarUrls,
  onSelectAccount,
}: {
  accounts: string[];
  activeAccount: string;
  anchorAccount: string;
  avatarUrls: FeedAvatarMap;
  onSelectAccount: (account: string) => void;
}) {
  return (
    <div className="hide-scrollbar flex gap-6 overflow-x-auto px-1 pb-1 sm:gap-8">
      {accounts.map((account) => (
        <StoryRing
          key={account}
          account={account}
          active={activeAccount === account}
          isAnchor={anchorAccount === account}
          profilePicUrl={avatarUrls[account]}
          onSelect={() => onSelectAccount(account)}
        />
      ))}
    </div>
  );
}

/* ═══════════════════════════════════════════
   2. PATTERN CARDS
   ═══════════════════════════════════════════ */

function PatternCard({
  pattern,
  patternIndex,
  onOpen,
}: {
  pattern: FeederFilePattern;
  patternIndex: number;
  onOpen: (pattern: FeederFilePattern) => void;
}) {
  const [, setFailedRailKeys] = useState<Set<string>>(
    () => new Set(pattern.proofs.filter((post) => isThumbnailFailureCached(post.post_key)).map((post) => post.post_key)),
  );

  const noteThumbnailUnavailable = useCallback((postKey: string) => {
    setFailedRailKeys((current) => {
      if (current.has(postKey)) return current;
      const next = new Set(current);
      next.add(postKey);
      return next;
    });
  }, []);

  const rankMetric = pattern.patternMetrics.find((m) => m.accent) || pattern.patternMetrics[0];

  // --- PREMIUM AUTOMATIC STACKED CAROUSEL STATE ---
  const [activeIndex, setActiveIndex] = useState(0);
  const totalProofs = pattern.proofs.length;

  useEffect(() => {
    if (totalProofs <= 1) return;
    const interval = setInterval(() => {
      setActiveIndex((prev) => (prev + 1) % totalProofs);
    }, 4000); // Swaps smoothly every 4 seconds
    return () => clearInterval(interval);
  }, [totalProofs]);

  return (
    <motion.button
      type="button"
      onClick={() => onOpen(pattern)}
      className="group/card relative overflow-hidden rounded-[26px] border border-black/[0.06] bg-[linear-gradient(135deg,#ffffff,#fff3f7)] p-5 text-left shadow-[0_12px_36px_-28px_rgba(15,23,42,0.5),inset_0_1px_0_rgba(255,255,255,0.82)] dark:border-white/[0.08] dark:bg-[linear-gradient(135deg,#18181b,#09090b)] dark:shadow-[0_14px_44px_-30px_rgba(0,0,0,0.9),inset_0_1px_0_rgba(255,255,255,0.08)] sm:p-6 md:p-7 lg:p-8"
      initial={{ opacity: 0, y: 16 }}
      animate={{ opacity: 1, y: 0 }}
      whileHover={{ y: -4 }}
      whileTap={{ scale: 0.992 }}
      transition={{ duration: 0.4, ease: APPLE_EASE, delay: patternIndex * 0.08 }}
    >
      {/* rank badge */}
      {rankMetric && (
        <div className="absolute right-5 top-5 z-10 rounded-full bg-[#E11D48] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-white shadow-[0_8px_20px_-10px_rgba(225,29,72,0.7)] sm:right-6 sm:top-6 md:right-7 md:top-7 lg:right-8 lg:top-8">
          {rankMetric.value}
        </div>
      )}

      {/* Two-column grid layout for pattern covers */}
      <div className="grid grid-cols-1 sm:grid-cols-[1fr_180px] md:grid-cols-[1fr_210px] lg:grid-cols-[1fr_240px] gap-6 md:gap-8 items-center w-full">
        
        {/* Left Column: Text detail + metrics */}
        <div className="flex flex-col h-full justify-between min-w-0">
          <div>
            {/* pattern label + tile label */}
            <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.16em] text-black/38 dark:text-white/34" style={{ fontFamily: 'monospace' }}>
              <span className="text-[#E11D48]">PATTERN {String(patternIndex + 1).padStart(2, '0')}</span>
              <span className="text-black/18 dark:text-white/14">/</span>
              <span>{pattern.pattern.tile_label}</span>
            </div>

            {/* headline */}
            <h2 className="mt-3 max-w-[480px] pr-14 sm:pr-4 text-[20px] font-black leading-[1.15] tracking-tight text-black dark:text-white sm:mt-4 sm:text-[22px] md:text-[24px] lg:text-[26px]">
              {pattern.pattern.tile_headline}
            </h2>

            {/* read snippet */}
            <p className="mt-2.5 max-w-[440px] text-[12.5px] sm:text-[13px] md:text-[13.5px] lg:text-[14px] font-bold leading-relaxed text-black/54 line-clamp-4 dark:text-white/50">
              {pattern.pattern.tile_read}
            </p>
          </div>

          {/* bottom: metrics + open cue */}
          <div className="mt-5 flex items-center justify-between gap-4 border-t border-black/[0.04] dark:border-white/[0.04] pt-4">
            <div className="flex flex-wrap items-center gap-x-1.5 sm:gap-x-3 gap-y-1 text-[9px] sm:text-[10px] md:text-[11px] font-black uppercase tracking-[0.03em] sm:tracking-[0.14em]">
              {pattern.patternMetrics.slice(1).map((m) => (
                <div key={`${pattern.pattern_id}:${m.label}`} className="flex items-center gap-1">
                  <span className="text-black/64 dark:text-white/58">{m.value}</span>
                  <span className="text-black/32 dark:text-white/28">{m.detail || m.label}</span>
                </div>
              ))}
            </div>

            <div className="flex shrink-0 items-center gap-1 text-[10.5px] sm:text-[11.5px] font-black uppercase tracking-[0.14em] text-black/36 transition group-hover/card:text-[#E11D48] dark:text-white/30 dark:group-hover/card:text-[#FB7185]">
              <span>Open</span>
              <ArrowUpRight size={12} strokeWidth={3} />
            </div>
          </div>
        </div>

        {/* Right Column: Premium circular stacked carousel (Big & Hero) */}
        <div className="flex items-center justify-center sm:justify-end shrink-0 select-none py-2">
          <div className="h-[125px] w-[230px] sm:h-[185px] sm:w-[200px] md:h-[230px] md:w-[230px] lg:h-[270px] lg:w-[260px] relative flex items-center justify-center">
            {pattern.proofs.map((post, i) => {
              let position: 'center' | 'left' | 'right' | 'hidden' = 'hidden';
              
              if (i === activeIndex) {
                position = 'center';
              } else if (i === (activeIndex + 1) % totalProofs) {
                position = 'right';
              } else if (i === (activeIndex - 1 + totalProofs) % totalProofs && totalProofs > 2) {
                position = 'left';
              }

              let positionClasses = "";
              
              if (position === 'center') {
                positionClasses = "z-35 scale-[1.14] translate-x-0 rotate-0 opacity-100 blur-0 shadow-[0_20px_48px_-12px_rgba(0,0,0,0.38)] dark:shadow-[0_28px_60px_-16px_rgba(0,0,0,0.9)] group-hover/card:scale-[1.20] group-hover/card:shadow-[0_26px_56px_-10px_rgba(0,0,0,0.48)] dark:group-hover/card:shadow-[0_34px_72px_-14px_rgba(0,0,0,0.98)] pointer-events-auto";
              } else if (position === 'left') {
                positionClasses = "z-10 scale-[0.82] -translate-x-[52px] sm:-translate-x-[58px] md:-translate-x-[74px] lg:-translate-x-[88px] rotate-[-10deg] opacity-48 blur-[1.5px] group-hover/card:-translate-x-[62px] sm:group-hover/card:-translate-x-[72px] md:group-hover/card:-translate-x-[88px] lg:group-hover/card:-translate-x-[102px] group-hover/card:rotate-[-15deg] group-hover/card:opacity-75 group-hover/card:blur-[0.5px] pointer-events-none";
              } else if (position === 'right') {
                positionClasses = "z-10 scale-[0.82] translate-x-[52px] sm:translate-x-[58px] md:translate-x-[74px] lg:translate-x-[88px] rotate-[10deg] opacity-48 blur-[1.5px] group-hover/card:translate-x-[62px] sm:group-hover/card:translate-x-[72px] md:group-hover/card:translate-x-[88px] lg:group-hover/card:translate-x-[102px] group-hover/card:rotate-[15deg] group-hover/card:opacity-75 group-hover/card:blur-[0.5px] pointer-events-none";
              } else {
                positionClasses = "z-0 scale-[0.6] translate-x-0 rotate-0 opacity-0 blur-[4px] pointer-events-none";
              }

              return (
                <div
                  key={post.post_key}
                  className={[
                    "absolute aspect-[4/5] w-[86px] sm:w-[110px] md:w-[124px] lg:w-[136px] shrink-0 overflow-hidden rounded-[16px] border border-black/10 dark:border-white/10 shadow-[0_4px_12px_rgba(0,0,0,0.1)] transition-all duration-[1000ms]",
                    positionClasses
                  ].join(" ")}
                  style={{
                    transitionTimingFunction: 'cubic-bezier(0.16, 1, 0.3, 1)'
                  }}
                >
                  <Thumb
                    post={post}
                    index={i + 1}
                    total={pattern.proofs.length}
                    showCaption={false}
                    onUnavailable={noteThumbnailUnavailable}
                    className="h-full w-full"
                  />
                  {position === 'center' && <CornerTicks color={ACCENT} size={8} inset={5} />}
                  {/* rank chip on thumb */}
                  <div className="absolute bottom-2 left-2 rounded-full bg-black/60 px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.15em] text-white">
                    {i + 1}/{pattern.proofs.length}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

      </div>
    </motion.button>
  );
}

/* ═══════════════════════════════════════════
   3. POPUP – MOBILE (card-based sections)
   ═══════════════════════════════════════════ */

function MobilePopup({
  pattern,
  proofIndex,
  setProofIndex,
  onClose,
}: {
  pattern: FeederFilePattern;
  proofIndex: number;
  setProofIndex: (i: number) => void;
  onClose: () => void;
}) {
  const proof = pattern.proofs[proofIndex];
  const total = pattern.proofs.length;
  const [showBreakdown, setShowBreakdown] = useState(false);
  const proofReadRef = useRef<HTMLElement>(null);
  const safeProof = proof ?? EMPTY_PROOF;
  const proofReadParagraphs = useMemo(() => readParagraphs(safeProof.post_read), [safeProof.post_read]);
  const proofSignalCards = useMemo(
    () => buildProofSignalCards(safeProof, pattern, proofIndex, total),
    [pattern, safeProof, proofIndex, total],
  );

  const goToProof = useCallback(
    (next: number, scrollTarget: 'current' | 'proof' = 'current') => {
      const wrapped = (next + total) % total;
      setProofIndex(wrapped);
      if (scrollTarget === 'proof') {
        requestAnimationFrame(() => {
          proofReadRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
      }
    },
    [total, setProofIndex],
  );

  if (!proof || total === 0) return null;

  return (
    <motion.div
      className="fixed inset-0 z-[1000] overflow-y-auto overflow-x-hidden bg-[#FAF9F6] dark:bg-[#09090b] text-[#111111] dark:text-white transition-colors duration-300"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.2, ease: APPLE_EASE }}
    >
      {/* fixed header */}
      <div className="fixed inset-x-0 top-0 z-[1020] bg-[linear-gradient(180deg,#FAF9F6_75%,transparent)] dark:bg-[linear-gradient(180deg,#09090b_75%,transparent)] pb-6 pt-[env(safe-area-inset-top)]">
        <div className="flex items-center justify-between px-3.5 pt-3">
          <div className="flex items-center gap-2 text-[9px] font-black uppercase tracking-[0.16em] text-black/44 dark:text-white/44 font-mono">
            <span className="text-[#E11D48] dark:text-[#FB7185]">{pattern.pattern.tile_label}</span>
            <span className="h-1.5 w-1.5 rounded-full bg-black/10 dark:bg-white/18" />
            <span>{pattern.account}</span>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="flex h-11 w-11 items-center justify-center rounded-full bg-black/5 text-black transition-all active:scale-95 hover:bg-[#E11D48] hover:text-white dark:bg-white/10 dark:text-white dark:hover:bg-[#E11D48] sm:backdrop-blur-md lg:backdrop-blur-none"
            aria-label="Close"
          >
            <X size={20} strokeWidth={3} />
          </button>
        </div>
      </div>

      <div
        style={{ paddingTop: 'calc(136px + env(safe-area-inset-top))' }}
        className="px-0 pb-[calc(32px+env(safe-area-inset-bottom))]"
      >

        {/* ── SECTION 1: Pattern Overview ── */}
        <section className="border-y border-x-0 border-black/[0.06] bg-white/90 px-5 py-6 shadow-sm dark:border-white/[0.06] dark:bg-zinc-900/60 sm:backdrop-blur-md lg:backdrop-blur-none">
          <div className="text-[11.5px] font-black uppercase tracking-[0.22em] text-[#E11D48] dark:text-[#FB7185] font-mono">
            Pattern overview
          </div>

          <h3 className="mt-3 text-[clamp(28px,6vw,34px)] font-serif font-extrabold leading-[1.08] tracking-tight text-black dark:text-white">
            {pattern.pattern.modal_headline}
          </h3>

          <p className="mt-4 text-[15px] font-black leading-[1.38] text-black/80 dark:text-white/76 border-l-4 border-[#E11D48] pl-3.5 italic">
            {pattern.pattern.the_hook}
          </p>

          {/* metrics strip */}
          <div className="mt-5 grid grid-cols-3 gap-3 border-t border-black/[0.08] dark:border-white/[0.07] pt-4">
            {pattern.patternMetrics.map((metric) => (
              <div key={`mf:${metric.label}`} className="min-w-0">
                <div className={['text-[17px] font-mono font-black leading-none', metric.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black dark:text-white'].join(' ')}>
                  {metric.value}
                </div>
                <div className="mt-1 text-[7.5px] font-black uppercase tracking-[0.06em] leading-tight text-black/40 dark:text-white/30">
                  {metric.detail || metric.label}
                </div>
              </div>
            ))}
          </div>

          {/* breakdown toggle */}
          <button
            type="button"
            onClick={() => setShowBreakdown(!showBreakdown)}
            className="mt-5 flex w-full items-center justify-between rounded-xl border border-black/10 dark:border-white/10 bg-white dark:bg-zinc-900 px-4.5 py-3.5 shadow-[0_4px_12px_rgba(0,0,0,0.03)] active:scale-[0.99] transition-all duration-200"
          >
            <div className="flex items-center gap-2.5">
              <span className="flex h-2 w-2 rounded-full bg-[#E11D48] dark:bg-[#FB7185]" />
              <span className="text-[11.5px] font-black uppercase tracking-[0.18em] text-black dark:text-white font-mono">Pattern breakdown</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-[9px] font-bold uppercase tracking-[0.08em] text-black/40 dark:text-white/40 font-mono">
                {showBreakdown ? 'Hide' : 'Expand'}
              </span>
              <ChevronRight
                size={14}
                strokeWidth={3}
                className={['text-[#E11D48] dark:text-[#FB7185] transition-transform duration-300', showBreakdown ? 'rotate-90' : ''].join(' ')}
              />
            </div>
          </button>
          <AnimatePresence>
            {showBreakdown && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.26, ease: APPLE_EASE }}
                className="overflow-hidden"
              >
                <div className="space-y-3 pt-4 text-[13px] font-bold leading-[1.52] text-black/60 dark:text-white/48">
                  {pattern.pattern.the_breakdown.map((item, i) => (
                    <div key={`mbd:${i}`} className="flex gap-3">
                      <span className="mt-0.5 text-[14px] font-black leading-none text-[#E11D48]">
                        {String(i + 1).padStart(2, '0')}
                      </span>
                      <p className="text-black/80 dark:text-zinc-200 font-extrabold">{item}</p>
                    </div>
                  ))}
                </div>

                <p className="mt-4 text-[13px] font-semibold leading-[1.52] text-black/50 dark:text-white/42">
                  {pattern.pattern.why_it_works}
                </p>

                <div className="mt-4 grid gap-3">
                  <div className="rounded-[14px] border border-black/[0.05] dark:border-white/[0.05] bg-black/[0.01] dark:bg-white/[0.025] p-4">
                    <div className="text-[11px] font-black uppercase tracking-[0.2em] text-[#E11D48] dark:text-[#FB7185] font-mono">Keep</div>
                    <ul className="mt-2.5 space-y-3">
                      {pattern.pattern.what_to_keep.map((item, i) => (
                        <li key={`mk:${i}`} className="flex gap-3 items-start">
                          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-lg bg-[#E11D48] text-[11px] font-black text-white shadow-sm">+</span>
                          <span className="text-[13.5px] font-bold leading-[1.3] text-black dark:text-zinc-100">{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                  <div className="rounded-[14px] border border-black/[0.05] dark:border-white/[0.05] bg-black/[0.005] dark:bg-white/[0.02] p-4">
                    <div className="text-[11px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">Kills</div>
                    <ul className="mt-2.5 space-y-3">
                      {pattern.pattern.what_kills_it.map((item, i) => (
                        <li key={`mki:${i}`} className="flex gap-3 items-start">
                          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-lg bg-black/10 dark:bg-white/10 text-[11px] font-black text-black/50 dark:text-white/40">-</span>
                          <span className="text-[13.5px] font-semibold leading-[1.3] text-black/80 dark:text-zinc-300">{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </section>

        {/* ── SECTION 2: Evidence ── */}
        <div className="mt-5 flex items-center gap-2 px-5 text-[13px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">
          Evidence · {total} proofs
        </div>
        <section className="mt-2 border-y border-x-0 border-black/[0.06] dark:border-white/[0.06] bg-white/90 dark:bg-zinc-900/60 px-5 py-4 shadow-sm">
          {/* proof selection cards */}
          <div className="hide-scrollbar -mx-1 flex gap-3 overflow-x-auto px-1 pb-1">
            {pattern.proofs.map((post, i) => {
              const active = i === proofIndex;
              const metric = post.metrics.find((m) => m.accent) || post.metrics[0];
              return (
                <button
                  key={post.post_key}
                  type="button"
                  onClick={() => goToProof(i, 'proof')}
                  className={[
                    'group/card w-[152px] shrink-0 overflow-hidden rounded-[14px] border text-left transition-all duration-200 active:scale-[0.98]',
                    active
                      ? 'border-[#E11D48]/56 bg-black/[0.02] dark:bg-white/[0.05] shadow-[0_4px_12px_rgba(225,29,72,0.1)]'
                      : 'border-black/[0.05] dark:border-white/[0.05] opacity-50 hover:opacity-75',
                  ].join(' ')}
                >
                  <div className="relative aspect-[4/5] overflow-hidden">
                    <Thumb post={post} className={['h-full w-full object-cover transition-all duration-300', active ? '' : 'grayscale opacity-60 contrast-[1.1]'].join(' ')} />
                    <div className="absolute left-1.5 top-1.5 rounded-full bg-[#E11D48] px-2 py-0.5 text-[7px] font-black uppercase tracking-[0.1em] text-white">
                      {metric.value}
                    </div>
                  </div>
                  <div className="px-3 py-2.5">
                    <div className="text-[10.5px] font-black uppercase leading-[1.1] tracking-[0.05em] text-black/80 dark:text-white/70">
                      {post.proof_label}
                    </div>
                    <div className="mt-1 text-[8.5px] font-black uppercase tracking-[0.12em] text-black/80 dark:text-zinc-200 font-mono">
                      Proof {i + 1}/{total}
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        </section>

        {/* ── SECTION 3: Proof Read ── */}
        <div className="mt-5 flex items-center gap-2 px-5 text-[13px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">
          Proof read
        </div>
        <section
          ref={proofReadRef}
          style={{ scrollMarginTop: 'calc(120px + env(safe-area-inset-top))' }}
          className="mt-2 border-y border-x-0 border-black/[0.06] dark:border-white/[0.06] bg-white/90 dark:bg-zinc-900/60 px-5 py-6 shadow-sm"
        >
          <h4 className="text-[25px] font-serif font-extrabold leading-[1.1] tracking-tight text-black dark:text-white">
            {proof.proof_headline}
          </h4>

          <a
            href={instagramPostUrl(proof.post_key, proof.post_url)}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-4 inline-flex h-11 items-center gap-2 rounded-full bg-[#E11D48] px-4 text-[10px] font-black uppercase tracking-[0.14em] text-white shadow-[0_10px_24px_-16px_rgba(225,29,72,0.8)] active:scale-[0.98]"
            aria-label="Open selected proof on Instagram"
          >
            <span>Open on Instagram</span>
            <ArrowUpRight size={13} strokeWidth={3} />
          </a>

          <div className="mt-5 border-t-2 border-[#E11D48] pt-3.5">
            <div className="text-[11px] font-black uppercase tracking-[0.2em] text-[#E11D48] dark:text-[#FB7185] font-mono">What clicked</div>
            <p className="mt-2 text-[clamp(16px,4.2vw,20px)] font-black leading-[1.3] text-[#E11D48] dark:text-[#FB7185] italic pl-3 border-l-2 border-[#E11D48]/30">
              &ldquo;{proof.what_clicked}&rdquo;
            </p>
          </div>

          {/* Bold Premium Stats Panel Grid */}
          <div className="mt-4 grid grid-cols-2 gap-2.5">
            {proofSignalCards.map((metric, i) => (
              <div
                key={`mstat:${proof.post_key}:${i}`}
                className={[
                  'relative min-w-0 rounded-xl border pl-3.5 pr-2 py-2.5 overflow-hidden',
                  metric.accent
                    ? 'border-[#E11D48]/20 bg-[#E11D48]/[0.02] dark:bg-[#E11D48]/[0.05]'
                    : 'border-black/[0.05] dark:border-white/[0.05] bg-black/[0.015] dark:bg-white/[0.015]',
                ].join(' ')}
              >
                <span className={['absolute left-0 top-0 bottom-0 w-[2.5px]', metric.accent ? 'bg-[#E11D48]' : 'bg-black/10 dark:bg-white/10'].join(' ')} />
                <div className={['text-[20px] font-mono font-black leading-none', metric.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black dark:text-white'].join(' ')}>
                  {metric.value}
                </div>
                <div className="mt-1 text-[7.5px] font-black uppercase tracking-[0.06em] leading-tight text-black/64 dark:text-zinc-400 font-mono">
                  {metric.label} {metric.detail ? `· ${metric.detail}` : ''}
                </div>
              </div>
            ))}
          </div>

          {/* Post Read: Phase Timeline on Mobile */}
          <div className="mt-4 space-y-3.5">
            {proofReadParagraphs.map((paragraph, i) => (
              <div key={`mpr:${proof.post_key}:${i}`} className="relative flex flex-col rounded-[18px] border border-black/[0.045] dark:border-white/[0.05] bg-black/[0.015] dark:bg-white/[0.015] p-4">
                <div className="flex items-center justify-between mb-3 text-[11.5px] font-black uppercase tracking-[0.18em] text-[#E11D48] dark:text-[#FB7185] font-mono border-b border-black/[0.05] dark:border-white/[0.05] pb-2">
                  <span>{['THE BUILD', 'THE MOVE', 'THE HOLD'][i] ?? `PHASE ${String(i + 1).padStart(2, '0')}`}</span>
                  <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
                </div>
                <p className="text-[13.5px] font-semibold leading-relaxed text-black/68 dark:text-zinc-300">
                  {paragraph}
                </p>
              </div>
            ))}
          </div>

          <div className="mt-5 rounded-[14px] border border-black/[0.05] dark:border-white/[0.05] bg-black/[0.01] dark:bg-white/[0.025] p-4">
            <div className="text-[11.5px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">Evidence</div>
            <ul className="mt-2.5 space-y-2 text-[12px] font-bold leading-[1.42] text-black/60 dark:text-white/46">
              {proof.evidence.map((e, i) => (
                <li key={`mev:${i}`} className="flex gap-2.5">
                  <span className="mt-[0.4em] h-1.5 w-1.5 shrink-0 rounded-full bg-[#E11D48]" />
                  <span>{e}</span>
                </li>
              ))}
            </ul>
          </div>
        </section>
      </div>
    </motion.div>
  );
}

/* ═══════════════════════════════════════════
   3b. POPUP – DESKTOP (magazine editorial)
   ═══════════════════════════════════════════ */

function DesktopPopup({
  pattern,
  proofIndex,
  setProofIndex,
  onClose,
}: {
  pattern: FeederFilePattern;
  proofIndex: number;
  setProofIndex: (i: number) => void;
  onClose: () => void;
}) {
  const proof = pattern.proofs[proofIndex];
  const total = pattern.proofs.length;
  const [direction, setDirection] = useState(0);
  const proofReadRef = useRef<HTMLElement>(null);
  const safeProof = proof ?? EMPTY_PROOF;
  const proofReadParagraphs = useMemo(() => readParagraphs(safeProof.post_read), [safeProof.post_read]);
  const proofSignalCards = useMemo(
    () => buildProofSignalCards(safeProof, pattern, proofIndex, total),
    [pattern, safeProof, proofIndex, total],
  );

  const patternPerformanceStats = useMemo(() => {
    const proofMetrics = pattern.proofs.flatMap((post) => post.metrics);
    const baselineMetric = proofMetrics.find((metric) => metric.label === 'Baseline');
    const viewsMetric = proofMetrics.find((metric) => metric.label === 'Views');
    return [
      ...pattern.patternMetrics,
      ...(baselineMetric ? [{ label: 'Lift proof', value: baselineMetric.value, detail: baselineMetric.detail }] : []),
      ...(viewsMetric ? [{ label: 'Reach proof', value: viewsMetric.value, detail: 'views in this pattern' }] : []),
    ];
  }, [pattern.patternMetrics, pattern.proofs]);

  const selectProof = useCallback(
    (next: number, scrollTarget: 'current' | 'proof' = 'current') => {
      if (next < 0 || next >= total) return;
      setDirection(next > proofIndex ? 1 : -1);
      setProofIndex(next);
      if (scrollTarget === 'proof') {
        requestAnimationFrame(() => {
          proofReadRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
      }
    },
    [proofIndex, total, setProofIndex],
  );

  if (!proof || total === 0) return null;

  return (
    <motion.div
      className="fixed inset-0 z-[1000] flex flex-col bg-[#FAF9F6] dark:bg-[#09090b] text-[#111111] dark:text-[#f4f4f5] transition-colors duration-300"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.2 }}
    >
      {/* close button */}
      <button
        type="button"
        onClick={onClose}
        className="fixed right-[clamp(20px,4vw,80px)] top-[clamp(20px,3.2vh,42px)] z-[1020] flex h-13 w-13 cursor-pointer items-center justify-center rounded-full border border-black/10 bg-white/90 text-black shadow-[0_8px_32px_-12px_rgba(0,0,0,0.3)] transition duration-200 hover:scale-108 hover:bg-[#E11D48] hover:text-white hover:border-[#E11D48] active:scale-95 dark:border-white/10 dark:bg-zinc-900/90 dark:text-white dark:hover:bg-[#E11D48] dark:hover:border-[#E11D48] sm:backdrop-blur-md lg:backdrop-blur-none"
        aria-label="Close"
      >
        <X size={22} strokeWidth={3} />
      </button>

      {/* ── HERO: full-width headline zone ── */}
      <div className="shrink-0 px-[clamp(20px,4vw,80px)] pb-6 pt-[clamp(22px,2.8vh,40px)]">
        {/* breadcrumb */}
        <div className="mb-5 flex items-center gap-3 text-[10px] font-black uppercase tracking-[0.25em] text-black/40 dark:text-white/34 font-mono">
          <span className="rounded-md border border-[#E11D48]/18 bg-[#E11D48]/10 px-3 py-1.5 text-[10.5px] font-black uppercase tracking-[0.14em] text-[#E11D48] dark:text-[#FB7185] shadow-[0_1px_3px_rgba(0,0,0,0.02)]">
            {pattern.pattern.tile_label}
          </span>
          <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
          <span>Pattern {String(proofIndex + 1).padStart(2, '0')}</span>
          <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48]" />
          <span className="text-[#E11D48] dark:text-[#FB7185] font-black">{pattern.account}</span>
        </div>

        {/* massive headline */}
        <h2
          className="max-w-[min(100%,1680px)] text-left text-[clamp(38px,3.5vw,62px)] font-serif font-semibold leading-[1.05] tracking-tight text-[#08080a] dark:text-zinc-50"
          style={{ textWrap: 'balance' }}
        >
          {pattern.pattern.modal_headline}
        </h2>

        {/* accent divider */}
        <div className="relative mt-8 h-[1px] w-full bg-black/[0.08] dark:bg-white/[0.08]">
          <div className="absolute left-0 top-0 h-[3px] -translate-y-1/3 w-[clamp(180px,24vw,400px)] bg-[#E11D48] shadow-[0_2px_10px_rgba(225,29,72,0.4)]" />
        </div>
      </div>

      {/* ── BODY: 2-panel below hero ── */}
      <div className="flex min-h-0 flex-1 overflow-hidden">

        {/* ── LEFT PANEL: scrollable content ── */}
        <div className="hide-scrollbar min-w-0 flex-1 overflow-y-auto">
          <div className="px-[clamp(20px,4vw,80px)] pb-12">

            {/* TOP SECTION: PATTERN READ + BREAKDOWN (Side-by-Side) */}
            <div className="grid items-stretch gap-[clamp(16px,1.8vw,28px)] grid-cols-1 xl:grid-cols-2">
              {/* Pattern Read */}
              <div className="relative flex h-full min-w-0 flex-col justify-between gap-6 overflow-hidden rounded-[24px] border border-black/[0.055] bg-white/88 p-[clamp(20px,2vw,32px)] shadow-[0_18px_42px_-36px_rgba(15,23,42,0.18)] transition-all duration-300 hover:shadow-lg dark:border-white/[0.06] dark:bg-zinc-900/82">
                <div className="flex flex-col gap-6">
                  <div className="flex items-center gap-2.5 text-[14px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono">
                    <span className="h-2 w-2 rounded-full bg-[#E11D48] animate-pulse" />
                    Pattern read
                  </div>
                  <p className="text-[clamp(28px,2.2vw,36px)] font-black leading-[1.08] text-[#E11D48] dark:text-[#FB7185] border-l-4 border-[#E11D48] pl-4 italic tracking-tight">
                    {pattern.pattern.the_hook}
                  </p>
                  <p className="text-[clamp(15px,1.05vw,18px)] font-bold leading-relaxed text-black/76 dark:text-zinc-200">
                    {pattern.pattern.why_it_works}
                  </p>
                </div>
                
                {/* Telemetry Footer */}
                <div className="pt-5 flex items-center justify-between border-t border-black/[0.06] dark:border-white/[0.06] text-[8.5px] font-mono uppercase tracking-[0.15em] text-black/32 dark:text-zinc-500">
                  <span>Core Mechanic</span>
                  <span>{pattern.accountMeta || 'Pattern Lift'}</span>
                </div>
              </div>

              {/* Breakdown */}
              <div className="flex h-full min-w-0 flex-col rounded-[24px] border border-black/[0.055] bg-white/88 p-[clamp(18px,1.7vw,28px)] shadow-[0_18px_42px_-36px_rgba(15,23,42,0.18)] transition-all duration-300 hover:shadow-lg dark:border-white/[0.06] dark:bg-zinc-900/82">
                <div className="mb-5 font-mono text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white">Breakdown</div>
                <div className="flex flex-col gap-3.5">
                  {pattern.pattern.the_breakdown.map((item, i) => (
                    <div key={`dbd:${i}`} className="grid min-h-0 grid-cols-[48px_minmax(0,1fr)] gap-4 rounded-[16px] border border-black/[0.03] bg-black/[0.015] p-3.5 transition-colors duration-300 hover:border-[#E11D48]/18 dark:border-white/[0.03] dark:bg-white/[0.015]">
                      <div className="text-[clamp(28px,1.9vw,40px)] font-black leading-none text-[#E11D48] dark:text-[#FB7185] font-mono drop-shadow-[0_2px_4px_rgba(225,29,72,0.15)]">
                        {String(i + 1).padStart(2, '0')}
                      </div>
                      <p className="self-center text-[clamp(14px,0.88vw,16px)] font-extrabold leading-[1.32] text-black/70 dark:text-zinc-300">{item}</p>
                    </div>
                  ))}
                </div>

                {/* Telemetry Footer */}
                <div className="mt-auto pt-5 flex items-center justify-between border-t border-black/[0.06] dark:border-white/[0.06] text-[8.5px] font-mono uppercase tracking-[0.15em] text-black/32 dark:text-zinc-500">
                  <span>Sequence Steps</span>
                  <span>Verified Signal</span>
                </div>
              </div>
            </div>

            {/* BOTTOM SECTION: GUARDRAILS (Full-Width with Keep & Kills Side-by-Side) */}
            <div className="mt-[clamp(16px,1.8vw,28px)]">
              {/* Guardrails */}
              <div className="relative flex min-w-0 flex-col justify-between gap-6 rounded-[24px] border border-black/[0.055] bg-white/88 p-[clamp(20px,2vw,32px)] shadow-[0_18px_42px_-36px_rgba(15,23,42,0.18)] dark:border-white/[0.06] dark:bg-zinc-900/82">
                <div>
                  <div className="mb-6 text-[14px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono">Guardrails</div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-8 items-stretch">
                    {pattern.pattern.what_to_keep.length > 0 && (
                      <div className="flex flex-col gap-3">
                        <div className="text-[13px] font-black uppercase tracking-[0.22em] text-[#E11D48] dark:text-[#FB7185] font-mono">Keep</div>
                        <ul className="space-y-4">
                          {pattern.pattern.what_to_keep.map((item, i) => (
                            <li key={`desktop-pattern-keep:${i}`} className="flex gap-4 items-start">
                              <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-[#E11D48] text-[13px] font-black text-white shadow-[0_2px_8px_rgba(225,29,72,0.3)]">
                                +
                              </span>
                              <span className="text-[clamp(14.5px,0.95vw,16.5px)] font-bold leading-[1.35] text-black dark:text-zinc-100">{item}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                    {pattern.pattern.what_kills_it.length > 0 && (
                      <div className={pattern.pattern.what_to_keep.length > 0 ? "border-t md:border-t-0 md:border-l border-black/[0.06] dark:border-white/[0.06] pt-5 md:pt-0 md:pl-8 flex flex-col gap-3" : "flex flex-col gap-3"}>
                        <div className="text-[13px] font-black uppercase tracking-[0.22em] text-black dark:text-white font-mono">Kills</div>
                        <ul className="space-y-4">
                          {pattern.pattern.what_kills_it.map((item, i) => (
                            <li key={`desktop-pattern-kill:${i}`} className="flex gap-4 items-start">
                              <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-black/10 dark:bg-white/10 text-[13px] font-black text-black/60 dark:text-white/60">
                                -
                              </span>
                              <span className="text-[clamp(14.5px,0.9vw,16px)] font-bold leading-[1.35] text-black/80 dark:text-zinc-300">{item}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                </div>

                {/* Telemetry Footer */}
                <div className="pt-5 flex items-center justify-between border-t border-black/[0.06] dark:border-white/[0.06] text-[8.5px] font-mono uppercase tracking-[0.15em] text-black/32 dark:text-zinc-500">
                  <span>Guardrail Limits</span>
                  <span>Atmospheric Constraint</span>
                </div>
              </div>
            </div>

            {/* ── EVIDENCE CARDS (sleek video-cover horizontal filmstrip) ── */}
            <div className="mt-8 border-t border-black/[0.08] dark:border-white/[0.08] pt-6">
                  <div className="mb-5 font-mono text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white">
                Evidence · {total} proofs — click to read
              </div>
              <div className="grid gap-4 lg:grid-cols-3">
                {pattern.proofs.map((post, i) => {
                  const active = i === proofIndex;
                  return (
                    <motion.button
                      key={post.post_key}
                      type="button"
                      onClick={() => selectProof(i, 'proof')}
                      whileHover={{ y: -6, transition: { duration: 0.2 } }}
                      whileTap={{ scale: 0.985 }}
                      className={[
                        'group/card relative w-full aspect-[16/10] overflow-hidden rounded-[22px] border text-left transition-all duration-300 cursor-pointer shadow-md hover:shadow-xl',
                        active
                          ? 'border-[#E11D48] ring-2 ring-[#E11D48]/30 dark:ring-[#E11D48]/40 shadow-[0_20px_40px_-20px_rgba(225,29,72,0.2)]'
                          : 'border-black/[0.08] dark:border-white/[0.08] bg-[#FAF9F6] dark:bg-[#141416]/20 opacity-75 hover:opacity-100'
                      ].join(' ')}
                    >
                      {/* Full-size media thumbnail behind gradient overlay */}
                      <Thumb post={post} className={['absolute inset-0 h-full w-full object-cover transition-all duration-500 ease-out group-hover/card:scale-[1.035]', active ? '' : 'grayscale opacity-60 contrast-[1.1] group-hover/card:grayscale-0 group-hover/card:opacity-100'].join(' ')} />
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.1)_40%,rgba(0,0,0,0.85)_100%)] z-10 transition-opacity duration-300 group-hover/card:bg-[linear-gradient(180deg,rgba(0,0,0,0.05)_30%,rgba(0,0,0,0.9)_100%)]" />

                      {/* Active Top Bezel Accent */}
                      {active && (
                        <span className="absolute top-0 inset-x-0 h-[3px] bg-[#E11D48] z-20 shadow-[0_2px_10px_rgba(225,29,72,0.6)]" />
                      )}

                      {/* Monospaced Index Tag */}
                      <div className="absolute left-4 top-4 z-20 rounded-md border border-white/10 bg-black/72 px-2 py-0.5 font-mono text-[8px] font-black uppercase tracking-[0.08em] text-white">
                        {String(i + 1).padStart(2, '0')}
                      </div>

                      {/* Editorial Content Overlay inside Thumbnail */}
                      <div className="absolute inset-x-0 bottom-0 z-20 p-5 flex flex-col justify-end">
                        <div className="text-[8px] font-black uppercase tracking-[0.2em] text-[#FB7185] font-mono leading-none">
                          {post.proof_label}
                        </div>
                        <h4 className="mt-2 text-[clamp(14px,1.15vw,17px)] font-black leading-[1.28] text-white transition-colors group-hover/card:text-[#FDA4AF]">
                          {post.proof_headline}
                        </h4>
                        <div className="mt-3 flex items-center gap-1.5 text-[8px] font-mono uppercase tracking-widest text-white/50 opacity-0 transition-all duration-300 translate-y-1 group-hover/card:opacity-100 group-hover/card:translate-y-0">
                          <span>View proof details</span>
                          <ChevronRight size={8} strokeWidth={3} className="transition-transform group-hover/card:translate-x-0.5 text-[#FB7185]" />
                        </div>
                      </div>
                    </motion.button>
                  );
                })}
              </div>
            </div>

            {/* ── PROOF READ ── */}
            <section ref={proofReadRef} className="mt-8 scroll-mt-4 border-t border-black/[0.08] dark:border-white/[0.08] pt-6">
              <div className="flex items-center justify-between gap-4">
                <div className="text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono">
                  Current read · Proof {proofIndex + 1}/{total}
                </div>
                <div className="rounded-full border border-[#E11D48]/20 bg-[#E11D48]/[0.05] px-3 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-[#E11D48] dark:text-[#FB7185] font-mono">
                  Selected proof
                </div>
              </div>

              <AnimatePresence mode="wait">
                <motion.div
                  key={`proof-read-transition:${proof.post_key}`}
                  initial={{ opacity: 0, y: 15 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -15 }}
                  transition={{ duration: 0.28, ease: APPLE_EASE }}
                >
                  <h3
                    className="mt-4 max-w-none text-[clamp(30px,2.4vw,44px)] font-serif font-semibold leading-[1.1] tracking-tight text-[#08080a] dark:text-zinc-50"
                    style={{ textWrap: 'balance' }}
                  >
                    {proof.proof_headline}
                  </h3>

                  {/* What Clicked: Full-Width Callout quote */}
                  <div className="relative mt-6 overflow-hidden rounded-[22px] border border-black/[0.055] dark:border-white/[0.06] bg-white/80 dark:bg-zinc-900/70 p-6 shadow-[0_18px_48px_-30px_rgba(225,29,72,0.15)] hover:shadow-md transition-shadow duration-300">
                    {/* top indicator bar */}
                    <span className="absolute top-0 inset-x-0 h-[3px] bg-[#E11D48] shadow-[0_1px_8px_rgba(225,29,72,0.4)]" />
                    <div className="text-[13.5px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono mb-3.5">What clicked</div>
                    <p className="text-[clamp(22px,1.6vw,28px)] font-black leading-[1.25] text-[#E11D48] dark:text-[#FB7185] italic pl-5 border-l-4 border-[#E11D48] tracking-tight">
                      &ldquo;{proof.what_clicked}&rdquo;
                    </p>
                  </div>

                  {/* Post Read: Full-Width Editorial Block with dynamic phase columns */}
                  <div className="mt-4 rounded-[22px] border border-black/[0.06] dark:border-white/[0.07] bg-white/80 dark:bg-zinc-900/70 p-[clamp(20px,2vw,32px)] shadow-lg hover:shadow-xl transition-shadow duration-300">
                    <div className="text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono mb-4.5">Post read</div>
                    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                      {proofReadParagraphs.map((paragraph, i) => (
                        <div 
                          key={`dpr:${proof.post_key}:${i}`} 
                          className="relative flex flex-col rounded-[20px] border border-black/[0.05] bg-white/90 p-[clamp(16px,1.5vw,24px)] shadow-[0_8px_30px_rgb(0,0,0,0.03)] transition-all duration-300 hover:border-[#E11D48]/30 hover:shadow-lg dark:border-white/[0.06] dark:bg-zinc-950/70 dark:shadow-none"
                        >
                          {/* Phase Indicator */}
                          <div className="flex items-center justify-between mb-4 text-[clamp(13px,0.95vw,14.5px)] font-black uppercase tracking-widest text-[#E11D48] dark:text-[#FB7185] font-mono border-b border-black/[0.05] dark:border-white/[0.05] pb-2.5">
                            <span>{['THE BUILD', 'THE MOVE', 'THE HOLD'][i] ?? `PHASE ${String(i + 1).padStart(2, '0')}`}</span>
                            <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
                          </div>
                          <p className="text-[clamp(15px,1.05vw,17.5px)] font-extrabold leading-[1.58] text-black/80 dark:text-zinc-200">
                            {paragraph}
                          </p>
                        </div>
                      ))}
                    </div>
                  </div>
                </motion.div>
              </AnimatePresence>
            </section>
          </div>
        </div>

        {/* ── RIGHT PANEL: proof + metrics sidebar ── */}
        <div className="hide-scrollbar w-[clamp(360px,28vw,520px)] shrink-0 overflow-y-auto border-l border-black/[0.08] dark:border-white/[0.08] bg-[linear-gradient(180deg,#ffffff_0%,#faf9f5_100%)] dark:bg-[linear-gradient(180deg,#0a0a0c_0%,#050506_100%)] transition-colors duration-300">
          <div className="px-[clamp(24px,2.4vw,42px)] pb-10 pt-1">

            {/* Feeder weight (pattern context before selected proof) */}
            <div>
              <div className="mb-4 text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono">Feeder weight</div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-3">
                {patternPerformanceStats.map((metric, index) => {
                  const val = metric.value || '';
                  const isLong = val.length > 8;
                  const sizeClass = isLong
                    ? 'text-[clamp(18px,1.4vw,22px)] font-bold leading-none'
                    : 'text-[clamp(26px,1.9vw,36px)] font-mono font-black leading-none';
                  return (
                    <div key={`dps:${metric.label}:${index}`} className="min-w-0 border-b border-black/[0.06] dark:border-white/[0.06] pb-2.5">
                      <div className={[sizeClass, metric.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-[#060607] dark:text-zinc-100'].join(' ')}>
                        {metric.value}
                      </div>
                      <div className="mt-1.5 text-[8.5px] font-black uppercase tracking-[0.16em] text-black/32 dark:text-zinc-500">
                        {metric.label} · {metric.detail || 'signal'}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Proof image card */}
            <div className="group/img relative mt-5 overflow-hidden rounded-[18px] border border-black/[0.07] dark:border-white/[0.07] bg-[#111] shadow-[0_20px_48px_-24px_rgba(0,0,0,0.45)]">
              <div className="relative aspect-[3/4] overflow-hidden">
                <AnimatePresence mode="popLayout" initial={false}>
                  <motion.div
                    key={`dimg:${proof.post_key}`}
                    className="absolute inset-0"
                    initial={{ x: direction > 0 ? 50 : -50, opacity: 0.5, scale: 0.97 }}
                    animate={{ x: 0, opacity: 1, scale: 1 }}
                    exit={{ x: direction > 0 ? -50 : 50, opacity: 0.5, scale: 0.97 }}
                    transition={{ duration: 0.28, ease: APPLE_EASE }}
                  >
                    <Thumb post={proof} className="h-full w-full" />
                  </motion.div>
                </AnimatePresence>

                {/* Precision CRT Scanline Grid Overlay */}
                <div className="absolute inset-0 pointer-events-none border border-white/5 bg-[linear-gradient(rgba(255,255,255,0.015)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.015)_1px,transparent_1px)] bg-[size:24px_24px] opacity-40 z-10" />

                {/* Diagnostic Central Reticle */}
                <div className="absolute inset-0 pointer-events-none z-10 flex items-center justify-center opacity-30">
                  <div className="h-14 w-14 border border-dashed border-white/20 rounded-full flex items-center justify-center">
                    <div className="h-2 w-2 bg-[#E11D48] rounded-full animate-ping" />
                    <div className="h-1.5 w-1.5 bg-[#E11D48] rounded-full absolute" />
                  </div>
                </div>

                <CornerTicks color={ACCENT} size={9} inset={7} />

                {/* Live Diagnostic Header Labels */}
                <div className="pointer-events-none absolute left-3 top-3 z-10 flex items-center gap-2 rounded-md border border-white/10 bg-black/78 px-2 py-0.5 font-mono text-[8.5px] uppercase tracking-wider text-white">
                  <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
                  <span>[ANALYZER FEED // LIVE]</span>
                </div>
                <div className="pointer-events-none absolute right-3 top-3 z-10 rounded-md border border-white/10 bg-black/78 px-2 py-0.5 font-mono text-[8.5px] uppercase tracking-wider text-white">
                  <span>FPS: 30 // HD // {proofIndex + 1}/{total}</span>
                </div>

                {/* always-visible blurred glass nav arrows */}
                <button type="button" onClick={() => selectProof((proofIndex - 1 + total) % total)}
                  className="absolute left-3 top-1/2 z-20 flex h-8 w-8 -translate-y-1/2 cursor-pointer items-center justify-center rounded-full border border-white/10 bg-black/58 text-white opacity-70 transition-all hover:scale-105 hover:opacity-100 active:scale-95"
                  aria-label="Previous proof">
                  <ChevronRight size={13} strokeWidth={2.8} className="rotate-180" />
                </button>
                <button type="button" onClick={() => selectProof((proofIndex + 1) % total)}
                  className="absolute right-3 top-1/2 z-20 flex h-8 w-8 -translate-y-1/2 cursor-pointer items-center justify-center rounded-full border border-white/10 bg-black/58 text-white opacity-70 transition-all hover:scale-105 hover:opacity-100 active:scale-95"
                  aria-label="Next proof">
                  <ChevronRight size={13} strokeWidth={2.8} />
                </button>

                {/* Telemetry bottom line */}
                <div className="pointer-events-none absolute bottom-14 left-4 z-20 flex items-center gap-2.5 rounded border border-white/5 bg-black/58 px-2 py-0.5 font-mono text-[8px] uppercase tracking-[0.16em] text-white/50">
                  <span>RATIO: 3:4</span>
                  <span className="h-1 w-1 rounded-full bg-white/20" />
                  <span>BITRATE: 4.8MB/S</span>
                  <span className="h-1 w-1 rounded-full bg-white/20" />
                  <span>KEY: {proof.post_key.slice(0, 8)}</span>
                </div>

                <a
                  href={instagramPostUrl(proof.post_key, proof.post_url)}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={(event) => event.stopPropagation()}
                  className="absolute bottom-3 right-3 z-30 inline-flex items-center gap-1.5 rounded-full border border-white/12 bg-black/68 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.14em] text-white shadow-[0_10px_24px_-16px_rgba(0,0,0,0.8)] transition hover:border-[#FB7185]/60 hover:bg-[#E11D48] hover:text-white active:scale-95"
                  aria-label="Open selected proof on Instagram"
                >
                  <span>Instagram</span>
                  <ArrowUpRight size={11} strokeWidth={3} />
                </a>

                <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10 bg-[linear-gradient(0deg,rgba(0,0,0,0.78),transparent_72%)] px-4 pb-3.5 pt-14">
                  <div className="text-[19px] font-black leading-[1] text-white">{proof.proof_label}</div>
                </div>
              </div>
            </div>

            {/* Signal Bites (selected proof context below thumbnail) */}
            <div className="mt-5 border-t border-black/[0.08] dark:border-white/[0.08] pt-4">
              <div className="mb-3.5 text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono">Signal bites</div>
              <div className="grid grid-cols-2 gap-2.5">
                {proofSignalCards.map((metric) => {
                  const val = metric.value || '';
                  const isLong = val.length > 8;
                  const sizeClass = isLong
                    ? 'text-[clamp(18px,1.4vw,22px)] font-bold leading-none'
                    : 'text-[clamp(26px,2vw,36px)] font-black leading-none';
                  return (
                    <div key={`dsb:${proof.post_key}:${metric.label}`}
                      className={[
                        'relative min-w-0 rounded-xl border pl-4 pr-3 py-3 overflow-hidden transition-all duration-300 hover:scale-[1.02] hover:shadow-md',
                        metric.accent
                          ? 'border-[#E11D48]/20 bg-[#E11D48]/[0.02] dark:bg-[#E11D48]/[0.05]'
                          : 'border-black/[0.06] dark:border-white/[0.06] bg-white dark:bg-zinc-900/60'
                      ].join(' ')}
                    >
                      {/* status accent strip */}
                      <span className={['absolute left-0 top-0 bottom-0 w-[3px]', metric.accent ? 'bg-[#E11D48]' : 'bg-black/10 dark:bg-white/10'].join(' ')} />

                      <div className={[sizeClass, 'font-mono tracking-tight', metric.accent ? 'text-[#E11D48] dark:text-[#FB7185] drop-shadow-[0_2px_4px_rgba(225,29,72,0.15)]' : 'text-[#060607] dark:text-zinc-100'].join(' ')}>
                        {metric.value}
                      </div>
                      <div className="mt-1.5 text-[8.5px] font-black uppercase tracking-[0.14em] text-black/40 dark:text-zinc-500">
                        {metric.label}{metric.detail ? ` · ${metric.detail}` : ''}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Post evidence list */}
            <div className="mt-6 border-t-2 border-[#E11D48]/20 dark:border-[#E11D48]/30 pt-5">
              <div className="mb-4 text-[14px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono">Post evidence</div>
              <div className="grid gap-2.5">
                {proof.evidence.map((item, i) => (
                  <div key={`desktop-rail-evidence:${proof.post_key}:${i}`} className="grid grid-cols-[30px_minmax(0,1fr)] gap-3 rounded-[14px] border border-black/[0.06] dark:border-white/[0.06] bg-white/70 dark:bg-zinc-900/40 p-3.5 shadow-sm hover:scale-[1.01] transition-transform duration-300">
                    <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-lg bg-[#E11D48]/10 text-[10px] font-black text-[#E11D48] dark:text-[#FB7185] font-mono">{i + 1}</span>
                    <span className="text-[clamp(13px,0.86vw,16px)] font-semibold leading-[1.38] text-black/70 dark:text-zinc-300 self-center">{item}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </motion.div>
  );
}

/* ═══════════════════════════════════════════
   3c. POPUP WRAPPER (responsive)
   ═══════════════════════════════════════════ */

function PatternPopup({
  pattern,
  onClose,
}: {
  pattern: FeederFilePattern | null;
  onClose: () => void;
}) {
  const [proofIndex, setProofIndex] = useState(0);
  const [isDesktop, setIsDesktop] = useState(
    () => typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches,
  );

  useEffect(() => {
    const mql = window.matchMedia('(min-width: 1024px)');
    const handler = (e: MediaQueryListEvent) => setIsDesktop(e.matches);
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, []);

  // Lock body scroll + keyboard
  useEffect(() => {
    if (!pattern) return undefined;
    const previous = document.documentElement.style.overflow;
    document.documentElement.style.overflow = 'hidden';

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
        return;
      }
      if (event.key === 'ArrowLeft') {
        setProofIndex((i) => Math.max(0, i - 1));
      } else if (event.key === 'ArrowRight') {
        setProofIndex((i) => Math.min(pattern.proofs.length - 1, i + 1));
      } else if (/^[1-9]$/.test(event.key)) {
        const idx = parseInt(event.key, 10) - 1;
        if (idx >= 0 && idx < pattern.proofs.length) setProofIndex(idx);
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.documentElement.style.overflow = previous;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [pattern, onClose]);

  if (typeof document === 'undefined') return null;

  return createPortal(
    <AnimatePresence>
      {pattern && isDesktop && (
        <DesktopPopup
          key="desktop-popup"
          pattern={pattern}
          proofIndex={proofIndex}
          setProofIndex={setProofIndex}
          onClose={onClose}
        />
      )}
      {pattern && !isDesktop && (
        <MobilePopup
          key="mobile-popup"
          pattern={pattern}
          proofIndex={proofIndex}
          setProofIndex={setProofIndex}
          onClose={onClose}
        />
      )}
    </AnimatePresence>,
    document.body,
  );
}

/* ═══════════════════════════════════════════
   4. ROOT COMPONENT
   ═══════════════════════════════════════════ */

export default function FeederFileClient({
  feedId,
  selectedHandle = 'all',
}: FeederFileClientProps) {
  const router = useRouter();
  const { appShellStyle, isStandaloneMode, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const bottomClearance = useTranslucentBrowserChrome
    ? 'calc(20px + env(safe-area-inset-bottom))'
    : isStandaloneMode
      ? 'calc(132px + env(safe-area-inset-bottom))'
      : 'calc(96px + env(safe-area-inset-bottom))';

  const [activeAccount, setActiveAccount] = useState<string>(
    () => accountForHandle(selectedHandle) ?? DEFAULT_ACCOUNT,
  );
  const [avatarUrls, setAvatarUrls] = useState<FeedAvatarMap>({});
  const [dbPatterns, setDbPatterns] = useState<FeederFilePattern[]>([]);
  const [patternsLoaded, setPatternsLoaded] = useState(false);

  const anchorAccount = useMemo(() => {
    return accountForHandle(selectedHandle) ?? DEFAULT_ACCOUNT;
  }, [selectedHandle]);

  const [activePattern, setActivePattern] = useState<FeederFilePattern | null>(null);
  const sourcePatterns = dbPatterns;
  const accounts = useMemo(() => {
    const next = Array.from(new Set(sourcePatterns.map((pattern) => pattern.account).filter(Boolean)));
    return next;
  }, [sourcePatterns]);
  const selectedHandleAccount = accountForHandle(selectedHandle);
  const selectedAccount = accounts.includes(activeAccount)
    ? activeAccount
    : selectedHandleAccount && accounts.includes(selectedHandleAccount)
      ? selectedHandleAccount
      : accounts[0] || selectedHandleAccount || DEFAULT_ACCOUNT;
  const visiblePatterns = useMemo(() => patternsForAccount(selectedAccount, sourcePatterns), [selectedAccount, sourcePatterns]);
  const renderablePatterns = useMemo(() => visiblePatterns.filter(hasRenderableProofs), [visiblePatterns]);
  const activeFeed = renderablePatterns[0] ?? visiblePatterns[0] ?? null;
  const activeAccountLabel = activeFeed?.account ?? (patternsLoaded ? selectedAccount || 'Feeder File' : 'Loading feeder file');
  const activeAccountMeta = activeFeed?.accountMeta ?? (
    patternsLoaded
      ? 'Waiting for D7-qualified pattern reads'
      : 'Reading official DB payloads'
  );

  useEffect(() => {
    let cancelled = false;
    setDbPatterns([]);
    setPatternsLoaded(false);

    async function loadFeederFilePatterns() {
      try {
        const params = new URLSearchParams({ feedId });
        const response = await fetch(`/api/feed/feeder-file?${params.toString()}`, { cache: 'no-store', credentials: 'include' });
        if (!response.ok) {
          if (!cancelled) setDbPatterns([]);
          return;
        }
        const payload = await response.json() as { patterns?: FeederFilePattern[] };
        if (!cancelled) setDbPatterns(Array.isArray(payload.patterns) ? payload.patterns : []);
      } catch {
        if (!cancelled) setDbPatterns([]);
      } finally {
        if (!cancelled) setPatternsLoaded(true);
      }
    }

    loadFeederFilePatterns();

    return () => {
      cancelled = true;
    };
  }, [feedId]);

  useEffect(() => {
    let cancelled = false;

    async function loadFeederAvatars() {
      try {
        const response = await fetch('/api/feed', { cache: 'no-store', credentials: 'include' });
        if (!response.ok) return;
        const payload = await response.json() as {
          feeds?: Array<{
            feeders?: Array<{
              handle?: string | null;
              profilePicUrl?: string | null;
            }>;
          }>;
        };
        const next: FeedAvatarMap = {};

        for (const feed of payload.feeds || []) {
          for (const feeder of feed.feeders || []) {
            const account = accountForHandle(feeder.handle);
            if (account && feeder.profilePicUrl && !next[account]) {
              next[account] = feeder.profilePicUrl;
            }
          }
        }

        if (!cancelled) setAvatarUrls(next);
      } catch {
        if (!cancelled) setAvatarUrls({});
      }
    }

    loadFeederAvatars();

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div
      className="relative min-h-[100dvh] overflow-x-hidden bg-[linear-gradient(180deg,#fff,#fafafa)] text-foreground dark:bg-[linear-gradient(180deg,#08080a,#050506)] dark:text-white"
      style={{ ...appShellStyle, paddingBottom: bottomClearance } as CSSProperties}
    >
      <main className="relative mx-auto flex w-full max-w-[1500px] flex-col px-3 pb-10 pt-[calc(16px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-5 lg:px-8">

        {/* ── sticky masthead + rings ── */}
        <div className="sticky top-[calc(env(safe-area-inset-top)+var(--pwa-top-fix,0px))] z-30">
          {/* title bar */}
          <div className="flex items-center gap-3 rounded-b-[22px] bg-white/86 pb-3 pt-1 dark:bg-[#08080a]/86 sm:pb-4 sm:backdrop-blur-xl lg:bg-white/96 lg:backdrop-blur-none lg:dark:bg-[#08080a]/96">
            <button
              type="button"
              onClick={() => router.push(`/?id=${feedId}`, { scroll: false })}
              className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[14px] border border-black/[0.06] bg-white/76 text-black/52 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/54 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]"
              aria-label="Back to feed dashboard"
            >
              <ArrowLeft size={20} strokeWidth={2.6} />
            </button>

            <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[14px] border border-[#E11D48]/18 bg-[#E11D48]/10 text-[#BE123C] shadow-[inset_0_1px_0_rgba(255,255,255,0.62)] dark:border-[#FB7185]/20 dark:bg-[#FB7185]/12 dark:text-[#FDA4AF] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
              <BrainCircuit size={18} strokeWidth={2.6} />
            </div>

            <div className="flex min-w-0 flex-1 items-baseline gap-2">
              <h1 className="truncate text-[20px] font-black leading-none tracking-normal text-black dark:text-white sm:text-[24px]">
                Feeder File
              </h1>
              <span className="hidden text-[9px] font-black uppercase tracking-[0.14em] text-black/30 dark:text-white/26 sm:inline">
                Content intelligence
              </span>
            </div>
          </div>

          {/* story rings strip */}
          <div className="rounded-b-[22px] bg-white/72 px-2 pb-4 pt-2 dark:bg-[#08080a]/72 sm:px-4 sm:backdrop-blur-xl lg:bg-white/94 lg:backdrop-blur-none lg:dark:bg-[#08080a]/94">
            <StoryStrip
              accounts={accounts}
              activeAccount={selectedAccount}
              anchorAccount={anchorAccount}
              avatarUrls={avatarUrls}
              onSelectAccount={setActiveAccount}
            />
          </div>
        </div>

        {/* ── "Reading @handle" hero section ── */}
        <section className="mt-6 sm:mt-8">
          <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.18em] text-[#E11D48]">
            <span className="h-2 w-2 rounded-full bg-[#E11D48]" />
            Reading
          </div>
          <h2 className="mt-2 text-[36px] font-black leading-none tracking-normal text-black dark:text-white sm:text-[48px] lg:text-[54px]">
            {activeAccountLabel}
          </h2>
          <p className="mt-2 text-[11px] font-black uppercase tracking-[0.12em] text-black/34 dark:text-white/28">
            {activeAccountMeta}
          </p>
        </section>

        {/* ── pattern cards grid ── */}
        <section className="mt-6 grid gap-8 sm:gap-10 sm:mt-8 xl:grid-cols-2">
          <AnimatePresence mode="popLayout">
            {renderablePatterns.map((pattern, i) => (
              <PatternCard
                key={`${pattern.account}:${pattern.pattern_id}`}
                pattern={pattern}
                patternIndex={i}
                onOpen={setActivePattern}
              />
            ))}
          </AnimatePresence>
          {renderablePatterns.length === 0 && (
            <div className="rounded-[22px] border border-black/[0.06] bg-white/76 p-6 text-[12px] font-bold leading-relaxed text-black/46 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/[0.08] dark:bg-white/[0.04] dark:text-white/40">
              {patternsLoaded
                ? 'Feeder file pattern reads will appear after D7-qualified posts generate fingerprints, post breakdowns, and a compiled feeder file.'
                : 'Loading feeder file pattern reads from the database.'}
            </div>
          )}
        </section>
      </main>

      {/* ── popup ── */}
      <PatternPopup
        key={activePattern ? `${activePattern.account}:${activePattern.pattern_id}` : 'closed'}
        pattern={activePattern}
        onClose={() => setActivePattern(null)}
      />
    </div>
  );
}
