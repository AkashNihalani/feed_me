'use client';

/* ─────────────────────────────────────────────────────────────
   READ 17 — the read as Story · Strategy · Split, for the feeders
   it has been counted for (Anuj and Lakmé for now: sample data in
   public/read17/<acct>.json, with its covers, story stills and
   breakdown frames beside it).

   React fetches the feeder's read once a session and mounts the
   plain-JS engine (read17Engine) on the page and on its layer at
   body level; the engine draws and owns everything inside both.
   It tells the header which part is on screen (onChapter), and
   the header's switch asks it to go to one (jumpRef).
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState, type MutableRefObject } from 'react';
import { createPortal } from 'react-dom';
import { appFont } from '@/lib/fonts';
import { mountRead17 } from './read17Engine';
import './readParts.css';

const BASE = '/read17/';
// the feeders read this way: handle → its read's file
const READ17: Record<string, string> = { 'anuj.mp4': 'anuj', lakmeindia: 'lakme' };
export const read17Of = (handle: string | null) => (handle ? READ17[handle] ?? null : null);

// the read's three parts, and the five pieces Strategy is cut into: the header names whichever is on screen
export type Chapter = 'story' | 'strategy' | 'split';
export type Part = 'story' | 'slices' | 'sides' | 'streaks' | 'followers' | 'formats' | 'split';
export const CHAPTERS: readonly Chapter[] = ['story', 'strategy', 'split'];
export const PARTS: readonly Part[] = ['story', 'slices', 'sides', 'streaks', 'followers', 'formats', 'split'];
const NAME: Record<Chapter | Part, string> = { story: 'Story', strategy: 'Strategy', split: 'Split', slices: 'Slices', sides: 'Sides', streaks: 'Streaks', followers: 'Followers', formats: 'Formats' };
export const chapterName = (chapter: Chapter) => NAME[chapter];
export const partName = (part: Part) => NAME[part];
export const chapterOf = (part: Part): Chapter => (part === 'story' || part === 'split' ? part : 'strategy');
// the part's number, as the page heads it (01 Story · 02 Strategy · 03 Split)
export const partNumber = (part: Part) => `0${CHAPTERS.indexOf(chapterOf(part)) + 1}`;

type Read = Record<string, unknown>;
type Mounted = { jump: (target: string) => void; unmount: () => void };
const reads: Record<string, Promise<Read>> = {};
const loaded: Record<string, Read> = {};
function load(acct: string) {
  reads[acct] ??= fetch(`${BASE}${acct}.json`)
    .then((response) => {
      if (!response.ok) throw new Error(`read17 ${acct}: ${response.status}`);
      return response.json() as Promise<Read>;
    })
    .then((read) => {
      loaded[acct] = read;
      return read;
    });
  // (a failed fetch is forgotten, so the next visit asks again)
  reads[acct].catch(() => { delete reads[acct]; });
  return reads[acct];
}

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
  const [data, setData] = useState<{ acct: string; read: Read } | null>(() => (loaded[acct] ? { acct, read: loaded[acct] } : null));
  const read = data && data.acct === acct ? data.read : loaded[acct] ?? null;
  useEffect(() => {
    if (read) return undefined;
    let live = true;
    load(acct).then((next) => { if (live) setData({ acct, read: next }); }).catch(() => {});
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
      base: BASE,
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
