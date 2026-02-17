import { NextResponse } from 'next/server';

export async function POST() {
  return NextResponse.json(
    { error: 'Deprecated. Apify results are processed by the engine worker.' },
    { status: 410 }
  );
}
