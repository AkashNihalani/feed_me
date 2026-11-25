'use client';

/* ─────────────────────────────────────────────────────────────
   READ ONE — Read as one page (the search, the files and the wall)
   for the feeders it has been read for (@anuj.mp4 for now). React
   fetches the read and mounts the plain-JS engine (readOneEngine) on
   the page and on its layer at body level; the engine draws and owns
   everything inside both. It tells the tab which file is open
   (onChrome), and the dock asks it to go back or to a time frame
   (handRef).
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState, type MutableRefObject } from 'react';
import { createPortal } from 'react-dom';
import { appFont } from '@/lib/fonts';
import { mountReadOne } from './readOneEngine';
import { keepOf, loadReadOne, loadedReadOnes, readOneOf, type ReadOneChrome, type ReadOneData } from './readOneData';
import './readOne.css';

export { readOneOf };
export type ReadOneHand = { frame: (days: number) => void; back: () => void };

export default function ReadOne({
  handle,
  minHeight,
  onChrome,
  handRef,
}: {
  handle: string;
  minHeight?: string;
  onChrome: (chrome: ReadOneChrome | null) => void;
  handRef: MutableRefObject<ReadOneHand | null>;
}) {
  const [root, setRoot] = useState<HTMLDivElement | null>(null);
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const [data, setData] = useState<{ handle: string; read: ReadOneData } | null>(null);
  const [failed, setFailed] = useState<string | null>(null);
  const [ask, setAsk] = useState(0);
  const read = data && data.handle === handle ? data.read : loadedReadOnes[handle] ?? null;
  useEffect(() => {
    if (read) return undefined;
    let live = true;
    loadReadOne(handle)
      .then((next) => { if (live) { setFailed(null); setData({ handle, read: next }); } })
      .catch(() => { if (live) setFailed(handle); });
    return () => { live = false; };
  }, [handle, read, ask]);

  // the engine keeps the callback it was mounted with: this one always reaches the latest
  const chromeRef = useRef(onChrome);
  useLayoutEffect(() => { chromeRef.current = onChrome; });

  useLayoutEffect(() => {
    if (!root || !layer || !read) return undefined;
    const mounted = mountReadOne({
      root,
      layer,
      data: read.drop,
      mortems: read.mortems,
      handle,
      keep: keepOf(handle),
      onChrome: (chrome: ReadOneChrome) => chromeRef.current(chrome),
    }) as ReadOneHand & { unmount: () => void };
    handRef.current = mounted;
    return () => {
      mounted.unmount();
      if (handRef.current === mounted) handRef.current = null;
      chromeRef.current(null);
    };
  }, [root, layer, read, handle, handRef]);

  const portalTarget = typeof document === 'undefined' ? null : document.body;
  return (
    <>
      {/* (its own arrival, and its own place in the page: the tab host leaves both be) */}
      <div data-tab-arrival="own" data-tab-scroll="own" className={`r1 r1-page ${appFont.variable}`} style={{ minHeight }}>
        <div ref={setRoot} />
        {failed === handle && !read ? (
          <div className="r1-failed" role="alert">
            <p>This read could not be loaded.</p>
            <button type="button" onClick={() => { setFailed(null); setAsk((n) => n + 1); }}>Try again</button>
          </div>
        ) : null}
      </div>
      {portalTarget ? createPortal(<div ref={setLayer} className={`r1 r1-layer ${appFont.variable}`} />, portalTarget) : null}
    </>
  );
}
