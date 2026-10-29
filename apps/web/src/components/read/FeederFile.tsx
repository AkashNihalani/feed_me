'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER FILE — the top of a read, a card above the rules and
   the wall: the feeder as a solar system. The core (who it is
   being) in the middle, the things in its posts round it on three
   orbits by how much they're its own, sized by how many posts
   they're in, standing above the floor (its usual post) and red
   when most of those landed, under it and grey when most faded;
   seen two ways (System, tilted, and Chart, from above).

   The system is the plain-JS engine (feederSystemEngine, three.js);
   a file without one ('version': 'system') still gets the earlier
   map (feederFileEngine). Either is loaded on demand with the
   feeder's own file so the rest of Read
   never waits for either, mounted on this section and torn down
   when it goes (the tab hiding under Activity included). Styled by
   feederFile.css.

   Preview data for now, one file per feeder that has one (FILES):
   the simulated run-3 read for Anuj (Sep 27 – Oct 11) and Lakmé
   (Sep 21 – Oct 1), 30 posts each, built by the worker's
   scripts/read_sim_to_page.py, with covers cut from the
   fingerprinted media (public/read-preview/<slug>-run3). The
   placeholder files sit beside them as *.placeholder.json.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useRef, type RefObject } from 'react';
import '@/components/read/feederFile.css';

// the simulated run-3 read: 30 posts each (P01–P30), covers cut from the fingerprinted media, and the feeder's profile
// picture (the system's centre; in production it is the feeder's own profile_pic_url)
const runCovers = (slug: string): Record<string, string> =>
  Object.fromEntries(Array.from({ length: 30 }, (_, i) => `P${String(i + 1).padStart(2, '0')}`).map((id) => [id, `/read-preview/${slug}-run3/${id}.jpg`]));

type FileEntry = { load: () => Promise<{ default: unknown }>; thumbs?: Record<string, string>; avatar?: string };
const FILES: Record<string, FileEntry> = {
  'anuj.mp4': { load: () => import('@/data/feederFileAnuj.json'), thumbs: runCovers('anuj'), avatar: '/read-preview/anuj-run3/avatar.jpg' },
  lakmeindia: { load: () => import('@/data/feederFileLakme.json'), thumbs: runCovers('lakme'), avatar: '/read-preview/lakme-run3/avatar.jpg' },
};

export const hasFeederFile = (handle: string | null | undefined): handle is string => Boolean(handle && FILES[handle]);
// the feeder's profile picture (the stories wear it too)
export const feederAvatar = (handle: string | null | undefined): string | null => (handle && FILES[handle]?.avatar) || null;

type Mounted = { unmount: () => void; open?: (id: string) => boolean; full?: (v: boolean, then?: () => void, instant?: boolean) => void };

/* target: a post, a thing or a rule to open (the map flies to it), again whenever targetKey changes. full: on a phone,
   be in the map's own full screen whenever it's shown (the Feeder File tucked away behind the read's stories), going in
   and out with no motion of its own: the layer it's in (layer, the read's) carries it as it opens and closes. shown:
   whether that layer is open */
export default function FeederFile({ handle, target = null, targetKey = 0, full = false, shown = true, layer = null, onReady }: {
  handle: string; target?: string | null; targetKey?: number; full?: boolean; shown?: boolean; layer?: RefObject<HTMLElement | null> | null; onReady?: () => void;
}) {
  const host = useRef<HTMLElement | null>(null);
  const api = useRef<Mounted | null>(null);
  const want = useRef({ target, targetKey, full, shown, onReady });
  const done = useRef(-1);
  useEffect(() => { want.current = { target, targetKey, full, shown, onReady }; });

  useEffect(() => {
    const el = host.current;
    const file = FILES[handle];
    if (!el || !file) return undefined;
    let mounted: Mounted | null = null;
    let gone = false;
    file.load().then(async (data) => {
      const d = data.default as { version?: string };
      // (in the read's layer: drawn whole at once, the layer carrying it in)
      const inLayer = layer?.current ?? null;
      const opts = { data: d, thumbs: file.thumbs || {}, avatar: file.avatar || null, layer: inLayer, intro: !inLayer, onReady: () => want.current.onReady?.() };
      if (d.version === 'system' || d.version === 'sections') {
        const { mountFeederSystem } = await import('@/components/read/feederSystemEngine');
        if (!gone) mounted = mountFeederSystem(el, opts) as Mounted;
      } else {
        const { mountFeederFile } = await import('@/components/read/feederFileEngine');
        if (!gone) mounted = mountFeederFile(el, opts) as Mounted;
      }
      if (gone || !mounted) return;
      api.current = mounted;
      const w = want.current;
      if (w.full && w.shown && window.innerWidth < 860) mounted.full?.(true, undefined, true);
      if (w.target) { done.current = w.targetKey; mounted.open?.(w.target); }
    });
    return () => {
      gone = true;
      api.current = null;
      mounted?.unmount();
    };
  // (the layer is the read's own, there from the first render on)
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [handle]);

  // on a phone, in full screen while the layer is shown and out of it once it's hidden, with no motion of its own
  const wasShown = useRef(shown);
  useEffect(() => {
    if (wasShown.current === shown) return;
    wasShown.current = shown;
    if (full && window.innerWidth < 860) api.current?.full?.(shown, undefined, true);
  }, [shown, full]);

  // a new target once it's up: the map flies to it
  useEffect(() => {
    if (!api.current || !target || done.current === targetKey) return;
    done.current = targetKey;
    api.current.open?.(target);
  }, [target, targetKey]);

  return <section ref={host} className="ff" aria-label="Feeder File" />;
}
