import { NextResponse } from 'next/server';

export async function GET() {
  return NextResponse.json(
    { error: 'Deprecated. Downloads are no longer supported via this endpoint.' },
    { status: 410 }
  );
}
