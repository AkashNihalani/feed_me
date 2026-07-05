'use client';

import { useEffect, useRef, useState } from 'react';

export type MetricKey =
  | 'views' | 'likes' | 'comments' | 'posts' | 'signals' | 'accounts' | 'feeds' | 'creators';

export type MetricAnchor = { value: number | null; ratePerSec: number };
export type LiveMetrics = Record<MetricKey, MetricAnchor>;

export type LivePlatformState = {
  metrics: LiveMetrics | null;
  measuredAt: string | null;
  fetchedAt: number; // client receive time (ms) — the extrapolation anchor
  ok: boolean;
  loading: boolean;
};

// Re-anchor hourly; the dashboard fills the gap with client-side steps.
const DEFAULT_REFRESH_MS = 60 * 60_000;

export function usePrefersReducedMotion(): boolean {
  const [reduced, setReduced] = useState(false);
  useEffect(() => {
    const mq = window.matchMedia('(prefers-reduced-motion: reduce)');
    const sync = () => setReduced(mq.matches);
    sync();
    mq.addEventListener('change', sync);
    return () => mq.removeEventListener('change', sync);
  }, []);
  return reduced;
}

/** Fetches real totals + per-metric growth rate, re-anchoring on a long interval. */
export function useLivePlatformStats(refreshMs: number = DEFAULT_REFRESH_MS): LivePlatformState {
  const [state, setState] = useState<LivePlatformState>({
    metrics: null,
    measuredAt: null,
    fetchedAt: Date.now(),
    ok: false,
    loading: true,
  });
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const load = async () => {
      try {
        const res = await fetch('/api/stats/public', { cache: 'no-store' });
        if (!res.ok) throw new Error(`stats ${res.status}`);
        const body = (await res.json()) as { metrics?: LiveMetrics; measuredAt?: string | null };
        if (!mountedRef.current) return;
        setState({
          metrics: body.metrics ?? null,
          measuredAt: body.measuredAt ?? null,
          fetchedAt: Date.now(),
          ok: true,
          loading: false,
        });
      } catch {
        if (!mountedRef.current) return;
        setState((prev) => ({ ...prev, ok: false, loading: false }));
      } finally {
        if (mountedRef.current) timer = setTimeout(load, refreshMs);
      }
    };

    load();
    return () => {
      mountedRef.current = false;
      if (timer) clearTimeout(timer);
    };
  }, [refreshMs]);

  return state;
}
