'use client';

import { useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { FireItem } from './types';

type FireCardProps = {
  item: FireItem;
};

type LayerSlide = {
  title: string;
  rows: string[];
};

function compact(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  const n = Math.abs(v);
  if (n >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(2)}B`;
  if (n >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return String(Math.round(v));
}

function pct(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  return `${Math.round(v)}%`;
}

function multiple(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  return `${v.toFixed(2)}x`;
}

function hourLabel(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  const hh = String(Math.max(0, Math.min(23, Math.round(v)))).padStart(2, '0');
  return `${hh}:00`;
}

function getNumber(item: FireItem, layerKey: keyof FireItem['layers'], ...keys: string[]): number | null {
  const layer = (item.layers[layerKey] ?? {}) as Record<string, unknown>;
  const payload = item.payload as Record<string, unknown>;
  for (const key of keys) {
    const a = layer[key];
    if (typeof a === 'number' && Number.isFinite(a)) return a;
    if (typeof a === 'string') { const n = Number(a.replace('%', '').trim()); if (Number.isFinite(n)) return n; }
    const b = payload[key];
    if (typeof b === 'number' && Number.isFinite(b)) return b;
    if (typeof b === 'string') { const n = Number(b.replace('%', '').trim()); if (Number.isFinite(n)) return n; }
  }
  return null;
}

function surfaceTone(trajectoryDelta: number | null): { percentileColor: string; deltaColor: string; deltaText: string | null } {
  if (trajectoryDelta == null || !Number.isFinite(trajectoryDelta)) {
    return { percentileColor: '#FFFFFF', deltaColor: '#FFFFFF', deltaText: null };
  }
  const x = Math.round(trajectoryDelta);
  if (x > 0) return { percentileColor: '#B4FF00', deltaColor: '#B4FF00', deltaText: `↑${x}` };
  if (x < 0) return { percentileColor: '#FF3B30', deltaColor: '#FF3B30', deltaText: `↓${Math.abs(x)}` };
  return { percentileColor: '#FFFFFF', deltaColor: '#FFFFFF', deltaText: null };
}

function buildSlides(item: FireItem): LayerSlide[] {
  const payload = (item.payload ?? {}) as Record<string, unknown>;
  const asRec = (v: unknown): Record<string, unknown> => (v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : {});

  const metrics = asRec(payload.metrics);
  const position = asRec(payload.position);
  const timing = asRec(payload.timing);
  const trajectory = asRec(payload.trajectory);
  const anchor = asRec(payload.anchor);
  const bestMetric = (typeof payload.best_metric === 'string' && payload.best_metric.trim() ? payload.best_metric.trim().toLowerCase() : 'views');

  const metricRows = ['views', 'likes', 'comments'].map((m) => {
    const md = asRec(metrics[m]);
    const val = compact(typeof md.value === 'number' ? md.value : null);
    const base = compact(typeof md.baseline === 'number' ? md.baseline : null);
    const mult = multiple(typeof md.multiple === 'number' ? md.multiple : null);
    return `${m.toUpperCase()}  ${val} vs ${base}  ${mult}`;
  });

  const bestData = asRec(metrics[bestMetric]);
  const rankFeed = typeof bestData.rank_feed === 'number' ? bestData.rank_feed : (typeof position.feed_rank === 'number' ? position.feed_rank : null);
  const bestInN = typeof bestData.best_in_last_n === 'number' ? bestData.best_in_last_n : null;
  const anchorHandle = typeof anchor.feeder_handle === 'string' ? anchor.feeder_handle : '';
  const anchorPct = typeof anchor.best_pct === 'number' ? anchor.best_pct : null;
  const anchorGap = typeof anchor.gap === 'number' ? anchor.gap : null;

  const posRows: string[] = [];
  posRows.push(rankFeed != null ? `#${Math.round(rankFeed)} IN FEED BY ${bestMetric.toUpperCase()}` : `INSUFFICIENT DATA FEED RANK`);
  posRows.push(bestInN != null ? `BEST IN LAST ${Math.max(1, Math.round(bestInN))} POSTS BY ${bestMetric.toUpperCase()}` : `INSUFFICIENT DATA RECENCY RANK`);
  if (anchorPct != null) {
    const gapTxt = anchorGap == null || !Number.isFinite(anchorGap) ? 'N/A' : `${anchorGap > 0 ? '+' : ''}${Math.round(anchorGap)} PTS`;
    posRows.push(`VS ANCHOR  @${anchorHandle.toUpperCase() || 'ANCHOR'} BEST: ${Math.round(anchorPct)}%  GAP: ${gapTxt}`);
  } else {
    posRows.push('NO RECENT ANCHOR DATA');
  }

  const cp = (item.checkpoint || 'D1').toUpperCase();
  const l3Rows: string[] = [];
  if (cp === 'D1') {
    const th = typeof timing.hour === 'number' ? timing.hour : null;
    const peakWindow = typeof timing.peak_window === 'string' && timing.peak_window.trim() ? timing.peak_window.trim().toUpperCase() : 'N/A';
    const hp = typeof timing.hour_percentile === 'number' ? timing.hour_percentile : null;
    const hm = typeof timing.hour_multiple === 'number' ? timing.hour_multiple : null;
    l3Rows.push(`POSTED AT ${hourLabel(th)} IST`);
    l3Rows.push(`PEAK WINDOW: ${peakWindow}`);
    l3Rows.push(`HOUR PERCENTILE: ${pct(hp)}`);
    l3Rows.push(`${hourLabel(th)} PRODUCES ${multiple(hm)} AVG`);
  } else {
    const d1 = typeof trajectory.d1 === 'number' ? trajectory.d1 : null;
    const d3 = typeof trajectory.d3 === 'number' ? trajectory.d3 : null;
    const d7 = typeof trajectory.d7 === 'number' ? trajectory.d7 : null;
    const deltaFrom = (a: number | null, b: number | null) => {
      if (a == null || b == null) return 'INSUFFICIENT DATA';
      const x = Math.round(a - b);
      return `${x > 0 ? '+' : ''}${x} PTS`;
    };
    l3Rows.push(`D1: ${pct(d1)}`);
    l3Rows.push(`D3: ${pct(d3)}  ${deltaFrom(d1, d3)}`);
    l3Rows.push(d7 == null ? 'D7: PENDING' : `D7: ${pct(d7)}  ${deltaFrom(d1, d7)}`);
    l3Rows.push(`CHECKPOINT ${cp}`);
  }

  return [
    { title: 'LAYER 1 · METRICS', rows: metricRows },
    { title: 'LAYER 2 · POSITION', rows: posRows },
    { title: cp === 'D1' ? 'LAYER 3 · TIMING' : 'LAYER 3 · JOURNEY', rows: l3Rows },
  ];
}


export function FireCard({ item }: FireCardProps) {
  const [focused, setFocused] = useState(false);
  const [index, setIndex] = useState(0);
  const [touchStartX, setTouchStartX] = useState<number | null>(null);
  const [touchStartY, setTouchStartY] = useState<number | null>(null);

  const tone = surfaceTone(item.trajectoryDeltaPercentile);
  const slides = useMemo(() => buildSlides(item), [item]);

  const handle = `@${(item.surfaceHandle || 'FEEDER').replace(/^@+/, '').toUpperCase()}`;
  const mediaType = item.surfaceMediaType?.toUpperCase() || 'POST';
  const checkpoint = item.checkpoint?.toUpperCase() || 'D1';
  const metricVal = compact(item.metricValue);

  const next = () => setIndex((v) => Math.min(v + 1, slides.length - 1));
  const prev = () => setIndex((v) => Math.max(v - 1, 0));

  return (
    <>
      {/* ═══ SURFACE CARD ═══ */}
      <motion.article
        layoutId={`fire-${item.id}`}
        whileTap={{ scale: 0.98 }}
        transition={{ duration: 0.24, ease: [0.2, 0.9, 0.2, 1] }}
        onClick={() => { setIndex(0); setFocused(true); }}
        className="relative w-full aspect-[4/5] overflow-hidden rounded-[24px] cursor-pointer select-none
          shadow-[0_4px_12px_rgba(0,0,0,0.06),0_12px_28px_-4px_rgba(0,0,0,0.12),0_32px_56px_-12px_rgba(0,0,0,0.14)]
          dark:shadow-[0_8px_16px_rgba(0,0,0,0.5),0_24px_48px_-8px_rgba(0,0,0,0.6)]
          border border-white/20 dark:border-white/[0.06]"
      >
        {/* Cover image */}
        {item.thumbnailUrl ? (
          <img src={item.thumbnailUrl} alt="signal" className="absolute inset-0 h-full w-full object-cover" loading="lazy" />
        ) : (
          <div className="absolute inset-0 bg-[#1a1a1a]" />
        )}

        {/* Neumorphic glass overlay — stronger for readability */}
        <div
          className="absolute inset-0"
          style={{
            background: [
              'linear-gradient(to bottom, rgba(0,0,0,0.3) 0%, rgba(0,0,0,0.12) 40%, rgba(0,0,0,0.5) 75%, rgba(0,0,0,0.82) 100%)',
              'radial-gradient(ellipse 120% 80% at 10% 10%, rgba(0,0,0,0.3) 0%, transparent 50%)',
              'radial-gradient(ellipse 120% 80% at 90% 90%, rgba(0,0,0,0.4) 0%, transparent 50%)',
            ].join(', '),
          }}
        />

        {/* Inner border highlight for glass depth */}
        <div
          className="pointer-events-none absolute inset-[1px] rounded-[23px]"
          style={{ boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.12), inset 0 -1px 0 rgba(0,0,0,0.15)' }}
        />

        {/* Percentile — deep embossed type */}
        <div className="absolute left-5 top-6 z-10 sm:left-6 sm:top-8">
          <motion.div
            initial={{ opacity: 0, y: 8 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, ease: [0.4, 0, 0.1, 1] }}
            className="text-[clamp(72px,22vw,150px)] font-black leading-[0.78] tracking-[-0.04em] md:text-[200px]"
            style={{
              color: tone.percentileColor,
              textShadow: `0 2px 4px rgba(0,0,0,0.4), 0 8px 24px rgba(0,0,0,0.3), 0 0 60px ${tone.percentileColor}22`,
            }}
          >
            {item.surfacePercentile == null ? '--%' : `${Math.round(item.surfacePercentile)}%`}
          </motion.div>

          {tone.deltaText && (
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.3, ease: [0.4, 0, 0.1, 1], delay: 0.04 }}
              className="-mt-2 ml-0.5 text-[clamp(24px,7vw,48px)] font-black leading-[0.92] md:text-[62px]"
              style={{
                color: tone.deltaColor,
                textShadow: `0 0 20px ${tone.deltaColor}44`,
              }}
            >
              {tone.deltaText}
            </motion.div>
          )}

          <motion.div
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.3, ease: [0.4, 0, 0.1, 1], delay: 0.06 }}
            className="mt-0.5 ml-0.5 text-[clamp(24px,7.5vw,52px)] font-medium lowercase leading-[0.92] text-white/80 md:text-[68px]"
            style={{ textShadow: '0 2px 8px rgba(0,0,0,0.5)' }}
          >
            {item.metricKey || 'metric'}
          </motion.div>
        </div>

        {/* Bottom bar — frosted glass stamp */}
        <div className="absolute bottom-0 inset-x-0 z-10">
          <div
            className="mx-3 mb-3 rounded-[14px] px-3 py-2.5 sm:mx-4 sm:mb-4 sm:px-4
              backdrop-blur-[20px] backdrop-saturate-[180%]
              bg-white/12 border border-white/15
              shadow-[inset_0_1px_0_rgba(255,255,255,0.15),inset_0_-1px_0_rgba(0,0,0,0.1),0_4px_12px_rgba(0,0,0,0.15)]
              dark:bg-white/8 dark:border-white/10"
          >
            <div className="flex items-center justify-between gap-2">
              <span className="truncate text-[10px] font-black uppercase tracking-[0.1em] text-white/90 sm:text-[11px]">
                {handle}
              </span>
              <div className="flex shrink-0 items-center gap-2">
                <span className="rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/70 sm:text-[9px]">
                  {mediaType}
                </span>
                <span className="rounded-[6px] bg-white/10 px-1.5 py-0.5 text-[8px] font-black uppercase tracking-[0.1em] text-white/70 sm:text-[9px]">
                  {checkpoint} · {metricVal}
                </span>
              </div>
            </div>
          </div>
        </div>
      </motion.article>

      {/* ═══ INTELLIGENCE LAYER (Full-screen Focus) ═══ */}
      <AnimatePresence>
        {focused && (
          <motion.div
            className="fixed inset-0 z-[80]"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.28, ease: [0.2, 0.9, 0.2, 1] }}
            onClick={() => setFocused(false)}
          >
            {/* Background */}
            <div className="absolute inset-0 bg-black/95 backdrop-blur-[24px]" />

            <motion.div
              layoutId={`fire-${item.id}`}
              className="absolute inset-0 overflow-hidden"
              transition={{ duration: 0.28, ease: [0.2, 0.9, 0.2, 1] }}
              onClick={(e) => e.stopPropagation()}
            >
              {/* Cover (dimmed) */}
              {item.thumbnailUrl ? (
                <img src={item.thumbnailUrl} alt="focus" className="absolute inset-0 h-full w-full object-cover opacity-20 blur-sm" />
              ) : null}
              <div className="absolute inset-0 bg-black/80" />

              <div
                className="relative z-10 flex h-full w-full flex-col"
                onTouchStart={(e) => {
                  const t = e.touches[0];
                  setTouchStartX(t.clientX);
                  setTouchStartY(t.clientY);
                }}
                onTouchEnd={(e) => {
                  if (touchStartX == null || touchStartY == null) return;
                  const t = e.changedTouches[0];
                  const dx = t.clientX - touchStartX;
                  const dy = t.clientY - touchStartY;
                  if (Math.abs(dy) > Math.abs(dx) && dy > 90) { setFocused(false); return; }
                  if (Math.abs(dx) < 50) return;
                  if (dx < 0) next();
                  if (dx > 0) prev();
                }}
              >
                {/* Header section */}
                <div className="px-5 pb-3 pt-[calc(16px+env(safe-area-inset-top))] sm:px-8">
                  {/* Percentile */}
                  <div
                    className="text-[80px] font-black leading-[0.8] tracking-[-0.04em] sm:text-[110px] md:text-[140px]"
                    style={{
                      color: tone.percentileColor,
                      textShadow: `0 0 40px ${tone.percentileColor}33, 0 4px 16px rgba(0,0,0,0.5)`,
                    }}
                  >
                    {item.surfacePercentile == null ? '--%' : `${Math.round(item.surfacePercentile)}%`}
                  </div>
                  {tone.deltaText && (
                    <div className="ml-1 text-[20px] font-black sm:text-[26px]" style={{ color: tone.deltaColor }}>
                      {tone.deltaText}
                    </div>
                  )}

                  {/* Stamp bar — frosted glass chip row */}
                  <div className="mt-3 flex flex-wrap items-center gap-1.5">
                    <span className="rounded-[8px] bg-white/8 border border-white/10 px-2.5 py-1 text-[10px] font-black uppercase tracking-[0.1em] text-white/80 sm:text-[11px]
                      shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]">
                      {handle}
                    </span>
                    <span className="rounded-[8px] bg-white/8 border border-white/10 px-2.5 py-1 text-[10px] font-black uppercase tracking-[0.1em] text-white/60 sm:text-[11px]
                      shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]">
                      {mediaType}
                    </span>
                    <span className="rounded-[8px] bg-white/8 border border-white/10 px-2.5 py-1 text-[10px] font-black uppercase tracking-[0.1em] text-white/60 sm:text-[11px]
                      shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]">
                      {checkpoint}
                    </span>
                    <span className="rounded-[8px] bg-white/8 border border-white/10 px-2.5 py-1 text-[10px] font-black uppercase tracking-[0.1em] text-white/60 sm:text-[11px]
                      shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]">
                      {metricVal}
                    </span>
                  </div>
                </div>

                {/* Intelligence slides */}
                <div className="relative flex-1 overflow-hidden">
                  <div
                    className="flex h-full transition-transform duration-300 ease-[cubic-bezier(0.22,1,0.36,1)]"
                    style={{ transform: `translateX(-${index * 100}vw)` }}
                  >
                    {slides.map((slide, i) => (
                      <section key={`${item.id}-slide-${i}`} className="w-screen shrink-0 px-5 pb-24 pt-4 sm:px-8">
                        {/* Layer title — frosted chip */}
                        <div className="mb-5 inline-flex rounded-[10px] bg-white/6 border border-white/8 px-3 py-1.5
                          shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]">
                          <span className="text-[11px] font-black tracking-[0.14em] text-white/40 sm:text-[12px]">
                            {slide.title}
                          </span>
                        </div>

                        <div className="space-y-4">
                          {slide.rows.map((row, idx) => (
                            <div
                              key={`${slide.title}-${idx}`}
                              className={
                                idx === 0
                                  ? 'text-[36px] font-black leading-[0.95] tracking-tight text-white sm:text-[48px] md:text-[56px]'
                                  : idx <= 2
                                    ? 'text-[18px] font-bold leading-snug text-white/85 sm:text-[20px]'
                                    : 'text-[14px] font-semibold text-white/50 sm:text-[16px]'
                              }
                              style={idx === 0 ? { textShadow: '0 2px 12px rgba(0,0,0,0.4)' } : undefined}
                            >
                              {row}
                            </div>
                          ))}
                        </div>
                      </section>
                    ))}
                  </div>
                </div>

                {/* Bottom nav — frosted glass bar */}
                <div className="absolute bottom-0 inset-x-0 pb-[calc(12px+env(safe-area-inset-bottom))]">
                  <div className="mx-4 flex items-center justify-between rounded-[16px]
                    bg-white/6 border border-white/8 px-4 py-3 backdrop-blur-[16px]
                    shadow-[inset_0_1px_0_rgba(255,255,255,0.06),0_-4px_12px_rgba(0,0,0,0.2)]
                    sm:mx-8">
                    <button
                      className="rounded-[10px] bg-white/8 border border-white/10 px-3.5 py-2 text-[10px] font-black uppercase tracking-[0.12em] text-white/70 transition-colors
                        hover:bg-white/12 disabled:opacity-25 sm:text-[11px]
                        shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]"
                      onClick={prev}
                      disabled={index === 0}
                    >
                      Prev
                    </button>
                    <div className="flex items-center gap-2">
                      {slides.map((_, i) => (
                        <button
                          key={i}
                          onClick={() => setIndex(i)}
                          className={`h-1.5 rounded-full transition-all duration-200 ${
                            i === index
                              ? 'w-6 bg-[#CCFF00] shadow-[0_0_8px_rgba(204,255,0,0.4)]'
                              : 'w-1.5 bg-white/25 hover:bg-white/40'
                          }`}
                        />
                      ))}
                    </div>
                    <button
                      className="rounded-[10px] bg-white/8 border border-white/10 px-3.5 py-2 text-[10px] font-black uppercase tracking-[0.12em] text-white/70 transition-colors
                        hover:bg-white/12 disabled:opacity-25 sm:text-[11px]
                        shadow-[inset_0_1px_0_rgba(255,255,255,0.06)]"
                      onClick={next}
                      disabled={index === slides.length - 1}
                    >
                      Next
                    </button>
                  </div>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
