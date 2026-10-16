'use client';

/* Diagnostic: `?probe` in the URL runs a scripted smoothness pass on the page (lib/perfProbe, loaded only then, so
   nobody else downloads it) and shows its numbers in a panel; the dev server also logs them (/api/dev/probe). Modes:
   `?probe` (scroll, switches, rail), `?probe=deep[&x=…]` (experiments), `?probe=css` / `?probe=vars` (what a restyle
   costs), `?probe&css=<old.css>` (old stylesheet against the current one, same page), `?probe=same&css=<old.css>`
   (does the old stylesheet draw anything differently). Renders nothing otherwise. */

import { useEffect, useState } from 'react';
import type { Report } from '@/lib/perfProbe';

export default function PerfProbe() {
  const [state, setState] = useState<'off' | 'running' | Report>('off');

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (!params.has('probe')) return;
    const deep = params.get('probe') === 'deep';
    const only = (params.get('x') || '').split(',').filter(Boolean);
    const cssOnly = params.get('probe') === 'css';
    let live = true;
    const start = window.setTimeout(() => {
      if (!live) return;
      setState('running');
      const note: string[] = [];
      import('@/lib/perfProbe').then(({ runProbe }) => runProbe(note, deep, only, cssOnly)).then((report) => {
        (window as unknown as { __probe: Report }).__probe = report;
        if (live) setState(report);
        fetch('/api/dev/probe', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(report) }).catch(() => {});
      });
    }, 0);
    return () => { live = false; window.clearTimeout(start); };
  }, []);

  if (state === 'off') return null;
  const box = { position: 'fixed', top: 'max(8px, env(safe-area-inset-top))', right: 8, zIndex: 2147483647, maxWidth: 'min(360px, calc(100vw - 16px))', padding: '8px 10px', borderRadius: 10, background: 'rgba(0,0,0,0.82)', color: '#fff', font: '10px/1.45 ui-monospace, monospace', pointerEvents: state === 'running' ? 'none' : 'auto' } as const;
  if (state === 'running') return <div style={box}>probe running… hands off</div>;
  return (
    <div style={box}>
      <div style={{ fontWeight: 700 }}>{state.hz} Hz · {state.size} @{state.dpr}x</div>
      <div>restyle all {state.costs.restyle}ms · relayout {state.costs.relayout}ms · {state.costs.nodes} nodes · {state.costs.rootVars} root vars · {state.costs.inlineVars} inline</div>
      {state.passes.map((pass) => (
        <div key={pass.name}>
          {pass.name.padEnd(20)} {String(pass.smooth).padStart(3)}% · {pass.fps}fps · hitch {pass.hitches} · worst {pass.worst}ms{pass.changes != null ? ` · ${pass.changes}Δ` : ''}{pass.split ? ` · js ${pass.split.script}/${pass.split.longestScript} · draw ${pass.split.frame}/${pass.split.longestFrame}` : ''}
        </div>
      ))}
      {state.css ? Object.entries(state.css).map(([name, ms]) => <div key={name}>{name.padEnd(16)} restyle {ms}ms</div>) : null}
      {state.same ? <div style={{ whiteSpace: 'pre-wrap' }}>{JSON.stringify(state.same).slice(0, 600)}</div> : null}
      {state.note.map((line) => <div key={line} style={{ color: '#fca5a5' }}>{line}</div>)}
    </div>
  );
}
