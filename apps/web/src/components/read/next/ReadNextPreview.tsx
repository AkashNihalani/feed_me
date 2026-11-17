'use client';

/* ─────────────────────────────────────────────────────────────
   READ (next), FOR REVIEW SIGNED OUT — the real page in the real
   chrome (SideRail, Dock, the Read tab's header), fed sample feeds.
   The chrome asks the server for the feeds' stream (who posted in
   the last day) and a signed-out answer sends the page to /login,
   so on this route only the app's /api calls are answered here:
   the feeds with a sample, everything else as unavailable (the
   rail then simply shows no fresh rings). ?theme=dark|light sets
   the theme for a screenshot.
   ───────────────────────────────────────────────────────────── */

import { useLayoutEffect } from 'react';
import { useSearchParams } from 'next/navigation';
import { buildRailFeeds, type RailFeed } from '@/components/read/readFeeds';
import ReadNext from './ReadNext';
import { Mark } from './Words';
import type { State } from './model';
import './readNext.css';

const SAMPLE = [
  {
    id: 'sample-comedy',
    title: 'Comedy',
    feeders: [
      { handle: 'anuj.mp4', profilePicUrl: '/read-preview/anuj-run3/avatar.jpg' },
      { handle: 'lakmeindia', profilePicUrl: '/read-preview/lakme-run3/avatar.jpg' },
      { handle: 'shreeecreates', profilePicUrl: null },
      { handle: 'bombaycomedy', profilePicUrl: null },
    ],
  },
  { id: 'sample-beauty', title: 'Beauty', feeders: [{ handle: 'lakmeindia', profilePicUrl: '/read-preview/lakme-run3/avatar.jpg' }] },
];
const FEEDS: RailFeed[] = buildRailFeeds(SAMPLE);

let shimmed = false;
function shim() {
  if (shimmed || typeof window === 'undefined') return;
  shimmed = true;
  const real = window.fetch.bind(window);
  window.fetch = (input: RequestInfo | URL, init?: RequestInit) => {
    const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
    const path = new URL(url, window.location.href).pathname;
    if (path === '/api/feed') return Promise.resolve(new Response(JSON.stringify({ feeds: SAMPLE }), { status: 200, headers: { 'content-type': 'application/json' } }));
    if (path.startsWith('/api/')) return Promise.resolve(new Response('{"error":"preview"}', { status: 503, headers: { 'content-type': 'application/json' } }));
    return real(input, init);
  };
}
shim();

export default function ReadNextPreview() {
  const params = useSearchParams();
  const theme = params.get('theme');
  useLayoutEffect(() => {
    if (theme !== 'dark' && theme !== 'light') return;
    const root = document.documentElement;
    root.classList.remove('dark', 'light');
    root.classList.add(theme);
  }, [theme]);
  // ?marks=1: every state of the file mark at three sizes, for review
  if (params.get('marks')) return <MarkSheet />;
  return <ReadNext seededFeeds={FEEDS} preview />;
}

function MarkSheet() {
  const states: { name: string; state: State; from?: State }[] = [
    { name: 'Hot', state: 'hot' },
    { name: 'Cold', state: 'cold' },
    { name: 'Flipped down', state: 'flipped', from: 'hot' },
    { name: 'Flipped up', state: 'flipped', from: 'cold' },
    { name: 'Mixed', state: 'mixed' },
    { name: 'Too few', state: 'too_few' },
  ];
  return (
    <div className="rn rn-marksheet" style={{ padding: 32, minHeight: '100dvh', display: 'block' }}>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(6, 140px)', gap: 24 }}>
        {states.map((st) => (
          <div key={st.name} style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            <span style={{ fontSize: 14, fontWeight: 700 }}>{st.name}</span>
            <span style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
              {(['s', 'm', 'l'] as const).map((sz) => (
                <Mark key={sz} state={st.state} from={st.from} size={sz} />
              ))}
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}
