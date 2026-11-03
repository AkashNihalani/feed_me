'use client';

/* ─────────────────────────────────────────────────────────────
   READ 17 — the read as its front (the line and the three figures
   it stands on), then Strategy and Split, for the feeders it has
   been counted for (Anuj and Lakmé for now: sample data in
   public/read17/<acct>.json, with its covers, story stills and
   breakdown frames beside it). The read's story plays on Feed,
   out of the feeder's face (Read17Story).

   React fetches the feeder's read once a session (read17Data) and
   mounts the plain-JS engine (read17Engine) on the page and on its
   layer at body level; the engine draws and owns everything inside
   both. It tells the header which part is on screen (onChapter),
   and the header's switch asks it to go to one (jumpRef).
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState, type MutableRefObject } from 'react';
import { createPortal } from 'react-dom';
import { appFont } from '@/lib/fonts';
import { mountRead17 } from './read17Engine';
import { READ17_BASE, loadRead, loadedReads, read17Of, type Read } from './read17Data';
import './readParts.css';

export { read17Of };

// the read's parts in the header (the front is the page's start, not a part), and the parts Strategy is cut into (the
// reader's six where the read has them, else the eight of its three acts): the header names whichever is on screen
export type Chapter = 'story' | 'strategy' | 'split' | 'recent' | 'ranks';
export type Part = 'story' | 'now' | 'score' | 'slices' | 'views' | 'likes' | 'comments' | 'follows' | 'streaks' | 'shifts' | 'frequency' | 'formats' | 'followers' | 'next' | 'split' | 'recent' | 'ranks';
export const CHAPTERS: readonly Chapter[] = ['strategy', 'recent', 'ranks'];
export const PARTS: readonly Part[] = ['story', 'now', 'score', 'slices', 'views', 'likes', 'comments', 'follows', 'streaks', 'shifts', 'frequency', 'formats', 'followers', 'next', 'split', 'recent', 'ranks'];
const NAME: Record<Chapter | Part, string> = { story: 'Story', strategy: 'Report', split: 'Wall', recent: 'Recent', ranks: 'Ranks', now: 'Now', score: 'Score', slices: 'Slices', views: 'Views', likes: 'Likes', comments: 'Comments', follows: 'Follows', streaks: 'Streaks', shifts: 'Shifts', frequency: 'Frequency', formats: 'Splits', followers: 'Followers', next: 'Next' };
export const chapterName = (chapter: Chapter) => NAME[chapter];
export const partName = (part: Part) => NAME[part];
// (the story isn't on the page any more: it plays on Feed, so nothing reports it; were it asked for, it is Strategy's)
export const chapterOf = (part: Part): Chapter => (part === 'ranks' ? 'ranks' : part === 'split' || part === 'recent' ? 'recent' : 'strategy');
// the part's number, as the page heads it (01 Report · 02 Recent · 03 Ranks: the wall, laid out either way)
export const partNumber = (part: Part) => `0${CHAPTERS.indexOf(chapterOf(part)) + 1}`;

type Mounted = { jump: (target: string) => void; unmount: () => void };

export default function Read17({
  acct,
  minHeight,
  onChapter,
  jumpRef,
}: {
  acct: string;
  minHeight?: string;
  onChapter: (part: Part) => void;
  jumpRef: MutableRefObject<((target: string) => void) | null>;
}) {
  const [root, setRoot] = useState<HTMLDivElement | null>(null);
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const [data, setData] = useState<{ acct: string; read: Read } | null>(() => (loadedReads[acct] ? { acct, read: loadedReads[acct] } : null));
  const read = data && data.acct === acct ? data.read : loadedReads[acct] ?? null;
  useEffect(() => {
    if (read) return undefined;
    let live = true;
    loadRead(acct).then((next) => { if (live) setData({ acct, read: next }); }).catch(() => {});
    return () => { live = false; };
  }, [acct, read]);

  // the engine keeps the callback it was mounted with: this one always reaches the latest
  const chapterRef = useRef(onChapter);
  useLayoutEffect(() => { chapterRef.current = onChapter; });

  useLayoutEffect(() => {
    if (!root || !layer || !read) return undefined;
    const mounted = mountRead17({
      root,
      layer,
      data: read,
      acct,
      base: READ17_BASE,
      onChapter: (part: Part) => chapterRef.current(part),
    }) as Mounted;
    jumpRef.current = mounted.jump;
    return () => {
      mounted.unmount();
      if (jumpRef.current === mounted.jump) jumpRef.current = null;
    };
  }, [root, layer, read, acct, jumpRef]);

  const portalTarget = typeof document === 'undefined' ? null : document.body;
  return (
    <>
      <div ref={setRoot} className={`r17 r17-page ${appFont.variable}`} style={{ minHeight }} />
      {portalTarget ? createPortal(<div ref={setLayer} className={`r17 r17-layer ${appFont.variable}`} />, portalTarget) : null}
    </>
  );
}
