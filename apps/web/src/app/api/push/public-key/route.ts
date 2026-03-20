import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';

export async function GET() {
  const publicKey = process.env.WEB_PUSH_VAPID_PUBLIC_KEY || process.env.NEXT_PUBLIC_WEB_PUSH_PUBLIC_KEY;
  if (!publicKey) {
    return NextResponse.json({ error: 'Web Push is not configured' }, { status: 503 });
  }

  return NextResponse.json({ publicKey });
}
