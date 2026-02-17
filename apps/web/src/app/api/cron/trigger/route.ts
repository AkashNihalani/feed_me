import { NextResponse } from 'next/server';

export const maxDuration = 10;

export async function GET() {
  return NextResponse.json(
    {
      error: 'Deprecated endpoint. Scheduling is handled only by Supabase pg_cron enqueue_daily_jobs at 23:30 IST.',
    },
    { status: 410 }
  );
}
