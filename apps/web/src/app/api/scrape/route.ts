import { NextResponse } from 'next/server';

export async function POST() {
  return NextResponse.json(
    { error: 'Deprecated. Scraping is handled by the engine worker.' },
    { status: 410 }
  );
}
