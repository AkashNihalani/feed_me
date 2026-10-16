import { NextResponse } from 'next/server';

/* Diagnostic sink for components/PerfProbe: prints a probe's report to the server log. Only on a local server (the dev
   server, or a production build started on this machine); a deployed app answers 404. */
export async function POST(request: Request) {
  const host = new URL(request.url).hostname;
  const local = process.env.NODE_ENV === 'development' || host === 'localhost' || host === '127.0.0.1';
  if (!local) return NextResponse.json({ ok: false }, { status: 404 });
  const report = await request.json().catch(() => null);
  console.log('[probe]', JSON.stringify(report));
  return NextResponse.json({ ok: true });
}
