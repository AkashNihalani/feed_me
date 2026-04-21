'use client';

import { RefObject, useEffect, useState } from 'react';

const RELEASE_THRESHOLD = 8;

export function useCompressedOnScroll(
  scrollRef: RefObject<HTMLElement | null>,
  useBrowserPageScroll: boolean,
  threshold = 24,
) {
  const [compressed, setCompressed] = useState(false);

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const source: HTMLElement | Window | null = useBrowserPageScroll
      ? window
      : scrollRef.current;
    if (!source) return;

    let raf = 0;
    let last = false;

    const read = () => (
      source === window
        ? window.scrollY
        : (source as HTMLElement).scrollTop
    );

    const sync = () => {
      const y = read();
      const next = last ? y > RELEASE_THRESHOLD : y > threshold;
      last = next;
      setCompressed((current) => (current === next ? current : next));
    };

    const onScroll = () => {
      if (raf) return;
      raf = window.requestAnimationFrame(() => {
        raf = 0;
        sync();
      });
    };

    sync();
    source.addEventListener('scroll', onScroll, { passive: true });

    return () => {
      source.removeEventListener('scroll', onScroll);
      if (raf) window.cancelAnimationFrame(raf);
    };
  }, [scrollRef, threshold, useBrowserPageScroll]);

  return compressed;
}
