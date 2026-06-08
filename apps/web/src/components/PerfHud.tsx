'use client';

// Dev-only FPS + long-task meter. Enabled automatically in development, or in
// any environment via `?perf=1` or localStorage `fm:perf=1`. Renders nothing
// otherwise (cheap to ship). Used to verify the 60fps / no-jitter work — watch
// for FPS dipping below ~58 and any long task > 50ms during scroll / tab switch
// / dialog open / filter.
import { useEffect, useState } from 'react';

export default function PerfHud() {
  const [enabled] = useState(() => {
    if (typeof window === 'undefined') return false;
    return process.env.NODE_ENV === 'development'
      || new URLSearchParams(window.location.search).has('perf')
      || window.localStorage.getItem('fm:perf') === '1';
  });
  const [fps, setFps] = useState(0);
  const [longTasks, setLongTasks] = useState(0);
  const [worstMs, setWorstMs] = useState(0);

  useEffect(() => {
    if (!enabled) return undefined;

    let raf = 0;
    let frames = 0;
    let last = performance.now();
    const tick = (now: number) => {
      frames += 1;
      if (now - last >= 500) {
        setFps(Math.round((frames * 1000) / (now - last)));
        frames = 0;
        last = now;
      }
      raf = window.requestAnimationFrame(tick);
    };
    raf = window.requestAnimationFrame(tick);

    let observer: PerformanceObserver | null = null;
    try {
      observer = new PerformanceObserver((list) => {
        for (const entry of list.getEntries()) {
          setLongTasks((count) => count + 1);
          setWorstMs((worst) => Math.max(worst, Math.round(entry.duration)));
        }
      });
      observer.observe({ entryTypes: ['longtask'] });
    } catch {
      // longtask not supported (Safari) — FPS meter still works.
    }

    return () => {
      window.cancelAnimationFrame(raf);
      observer?.disconnect();
    };
  }, [enabled]);

  if (!enabled) return null;

  const fpsColor = fps >= 58 ? '#22c55e' : fps >= 45 ? '#eab308' : '#ef4444';
  return (
    <div
      style={{
        position: 'fixed',
        bottom: 6,
        left: 6,
        zIndex: 2147483647,
        pointerEvents: 'none',
        fontFamily: 'ui-monospace, monospace',
        fontSize: 10,
        lineHeight: 1.35,
        padding: '3px 7px',
        borderRadius: 6,
        background: 'rgba(0,0,0,0.62)',
        color: '#fff',
        userSelect: 'none',
      }}
    >
      <span style={{ color: fpsColor, fontWeight: 700 }}>{fps} fps</span>
      {'  '}
      <span style={{ color: longTasks ? '#f87171' : '#9ca3af' }}>
        LT {longTasks}/{worstMs}ms
      </span>
    </div>
  );
}
