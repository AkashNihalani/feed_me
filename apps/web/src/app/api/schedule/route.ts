import { NextResponse } from 'next/server';

export async function GET() {
  return NextResponse.json(
    { error: 'Deprecated. Scheduling is handled by Supabase pg_cron.' },
    { status: 410 }
  );
}

export async function POST() {
  return NextResponse.json(
    { error: 'Deprecated. Scheduling is handled by Supabase pg_cron.' },
    { status: 410 }
  );
}

export async function DELETE() {
  return NextResponse.json(
    { error: 'Deprecated. Scheduling is handled by Supabase pg_cron.' },
    { status: 410 }
  );
}
