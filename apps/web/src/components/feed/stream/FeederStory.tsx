'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER STORY — a feeder's read, as an Instagram story on Feed.
   The profile's big face (ProfileHeader's FaceBlock) wears the
   story ring while there's a read whose story they haven't
   watched; a tap opens the story out of the face (Read17Story,
   the read's own story engine) and it shrinks back in when it
   shuts, after which the face wears its usual ring (the last 24
   hours') until the next read.

   Once the profile has settled, the read's file and the story's
   code are fetched in the background, so a tap opens it at once.
   ───────────────────────────────────────────────────────────── */

import dynamic from 'next/dynamic';
import { useCallback, useEffect, useState } from 'react';
import { loadRead, loadedReads, read17Avatar, read17Of, readId } from '@/components/read/read17/read17Data';

const Read17Story = dynamic(() => import('@/components/read/read17/Read17Story'), { ssr: false });
const warmStory = () => import('@/components/read/read17/Read17Story');

const SEEN = (acct: string) => `read17.storySeen.${acct}`;
function seenOf(acct: string) {
  try { return localStorage.getItem(SEEN(acct)); } catch { return null; }
}
function markSeen(acct: string, id: string | null) {
  try { localStorage.setItem(SEEN(acct), id ?? '1'); } catch {}
}

export type FeederStoryState = {
  acct: string | null;
  // a story they haven't watched: the face wears the ring
  ring: boolean;
  open: boolean;
  start: () => void;
  closed: () => void;
};

export function useFeederStory(handle: string | null | undefined): FeederStoryState {
  const acct = read17Of(handle);
  const [readKey, setReadKey] = useState<string | null>(() => (acct && loadedReads[acct] ? readId(loadedReads[acct]) : null));
  const [seen, setSeen] = useState<string | null>(null);
  const [open, setOpen] = useState(false);

  useEffect(() => {
    if (!acct) return undefined;
    setSeen(seenOf(acct));
    const warm = () => {
      loadRead(acct).then((read) => setReadKey(readId(read))).catch(() => {});
      warmStory().catch(() => {});
    };
    if (typeof window.requestIdleCallback === 'function') {
      const id = window.requestIdleCallback(warm, { timeout: 2500 });
      return () => window.cancelIdleCallback(id);
    }
    const id = window.setTimeout(warm, 1200);
    return () => window.clearTimeout(id);
  }, [acct]);

  const start = useCallback(() => setOpen(true), []);
  const closed = useCallback(() => {
    setOpen(false);
    if (!acct) return;
    const id = loadedReads[acct] ? readId(loadedReads[acct]) : readKey;
    markSeen(acct, id);
    setSeen(id ?? '1');
  }, [acct, readKey]);

  // (watched: the mark names this read, or, before the read's file has come, any read at all)
  const watched = seen != null && (readKey == null || seen === readKey);
  return { acct, ring: acct != null && !watched, open, start, closed };
}

export function FeederStoryLayer({ acct, handle, face, onClosed }: {
  acct: string;
  handle: string | null | undefined;
  face: HTMLElement | null;
  onClosed: () => void;
}) {
  return <Read17Story acct={acct} face={face} avatar={read17Avatar(handle)} onClosed={onClosed} />;
}
