import { NextResponse } from 'next/server';

export async function POST() {
  return NextResponse.json(
    { error: 'Deprecated. FeedMe now routes scraping through Bright Data in the engine worker.' },
    { status: 410 }
  );
}
