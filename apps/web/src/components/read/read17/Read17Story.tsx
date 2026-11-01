'use client';

/* ─────────────────────────────────────────────────────────────
   READ 17's STORY, on Feed: the read's story alone, mounted only
   while it's open. It mounts the engine in its 'story' mode on a
   layer at body level, opens the story out of the face it was
   given (it grows from the face and shrinks back into it), and
   tells its owner when it has shut (onClosed), which unmounts it.
   Loaded on first need (FeederStory), so Feed carries neither the
   engine nor its styles until a feeder's face is tapped.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState, type SyntheticEvent } from 'react';
import { createPortal } from 'react-dom';
import { appFont } from '@/lib/fonts';
import { mountRead17 } from './read17Engine';
import { READ17_BASE, loadRead, loadedReads, type Read } from './read17Data';
import './readParts.css';

type Mounted = { open: () => void; close: () => void; unmount: () => void };
// (the engine handles every touch on the story itself; none of them reaches the page under it)
const keep = (event: SyntheticEvent) => event.stopPropagation();

export default function Read17Story({ acct, face, avatar, onClosed }: {
  acct: string;
  // the face the story opens out of and shuts back into
  face: HTMLElement | null;
  avatar: string | null;
  onClosed: () => void;
}) {
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const [read, setRead] = useState<Read | null>(() => loadedReads[acct] ?? null);
  const closedRef = useRef(onClosed);
  useLayoutEffect(() => { closedRef.current = onClosed; });

  useEffect(() => {
    if (read) return undefined;
    let live = true;
    loadRead(acct).then((next) => { if (live) setRead(next); }).catch(() => { if (live) closedRef.current(); });
    return () => { live = false; };
  }, [acct, read]);

  useLayoutEffect(() => {
    if (!layer || !read) return undefined;
    const mounted = mountRead17({
      root: null,
      layer,
      data: read,
      acct,
      base: READ17_BASE,
      onChapter: null,
      avatar,
      mode: 'story',
      face,
      onClose: () => closedRef.current(),
    }) as Mounted;
    mounted.open();
    return () => mounted.unmount();
  }, [layer, read, acct, face, avatar]);

  if (typeof document === 'undefined') return null;
  return createPortal(
    <div
      ref={setLayer}
      className={`r17 r17-layer ${appFont.variable}`}
      onClick={keep}
      onPointerDown={keep}
      onPointerUp={keep}
      onPointerMove={keep}
      onTouchStart={keep}
      onTouchMove={keep}
      onTouchEnd={keep}
      onWheel={keep}
    />,
    document.body,
  );
}
