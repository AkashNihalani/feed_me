import { NextResponse } from 'next/server';

export const maxDuration = 10;

export async function GET() {
  return NextResponse.json(
    {
      error:
        'Deprecated endpoint. Scheduling is handled by Supabase pg_cron (daily enqueue at 00:05 IST, 3-day lookback, 18:30 IST checkpoint due times, plus repair watchdogs).',
    },
    { status: 410 }
  );
}
