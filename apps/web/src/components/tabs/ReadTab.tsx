'use client';

/* ─────────────────────────────────────────────────────────────
   READ — the breakdown behind a feeder: the trajectory of its runs,
   then every post as a wall you read by Read, Repeat or Rank.

   The page is driven by a plain-JS engine (components/read/readerEngine)
   ported from the approved prototype; this component only provides the
   app's chrome around it: the shared glass header capsule (AppHeader),
   the page container, and a body-level layer for the drop panel, the
   detail sheet and the hover tip. The engine mounts while the tab is
   visible and fully tears down when it is hidden (Activity).

   Data is sample data for now (two feeders), see data/readerPreview.json.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useState } from 'react';
import { createPortal } from 'react-dom';
import { Space_Grotesk } from 'next/font/google';
import { AppHeader } from '@/components/shell/AppShell';
import { mountReader } from '@/components/read/readerEngine';
import { HEADER_HTML, LAYER_HTML } from '@/components/read/readMarkup';
import { acquireRootPageScroll } from '@/lib/rootScrollMode';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import readerData from '@/data/readerPreview.json';
import '@/components/read/readTab.css';

const readFont = Space_Grotesk({
  subsets: ['latin'],
  weight: ['400', '500', '600', '700'],
  variable: '--rd-font',
  display: 'swap',
});

// Stable objects: React re-applies dangerouslySetInnerHTML whenever the object changes, which would wipe
// everything the engine has written into the header and the layer.
const HEADER_MARKUP = { __html: HEADER_HTML };
const LAYER_MARKUP = { __html: LAYER_HTML };

export default function ReadTab() {
  const { appShellStyle, useBrowserPageScroll } = useMobileImmersiveViewport();
  const [compressed, setCompressed] = useState(false);
  const [head, setHead] = useState<HTMLDivElement | null>(null);
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  // the overlays live at body level so position: fixed is never trapped by a transformed tab container
  const portalTarget = typeof document === 'undefined' ? null : document.body;

  // the engine measures and scrolls the window at every width, so the page scrolls while Read is visible.
  // The body stays overflow-visible so the root element is the only scroller (otherwise the body becomes
  // a scroll container of its own and the sticky lens on desktop stops sticking).
  // Re-applied whenever the shared viewport hook switches modes, because its own page-scroll lease resets
  // the body's overflow.
  useEffect(() => {
    const release = acquireRootPageScroll();
    const body = document.body;
    const prev = body.style.overflow;
    body.style.overflow = 'visible';
    return () => {
      body.style.overflow = prev;
      release();
    };
  }, [useBrowserPageScroll]);

  // mount before paint, so a stale wall from the last visit never shows
  useLayoutEffect(() => {
    if (!head || !layer) return undefined;
    // the engine's cleanup closes and empties every overlay in the layer, so it can stay in the body hidden
    const unmount = mountReader({ data: readerData, layer, setCompressed });
    return () => {
      unmount();
      setCompressed(false);
    };
  }, [head, layer]);

  return (
    <>
      <AppHeader id="read" compressed={compressed}>
        <div
          ref={setHead}
          className={`rd rd-head ${readFont.variable}`}
          dangerouslySetInnerHTML={HEADER_MARKUP}
        />
      </AppHeader>
      {/* min-height only: Read scrolls the page at every width, so it never takes the fixed shell height
          that tabs with their own inner scroller use on desktop */}
      <div className={`rd rd-page ${readFont.variable}`} style={{ minHeight: appShellStyle.minHeight }}>
        <div className="wrap" id="rd-main" />
      </div>
      {portalTarget
        ? createPortal(
          <div
            ref={setLayer}
            id="rd-layer"
            className={`rd rd-layer ${readFont.variable}`}
            dangerouslySetInnerHTML={LAYER_MARKUP}
          />,
          portalTarget,
        )
        : null}
    </>
  );
}
