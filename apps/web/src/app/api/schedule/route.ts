import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@/lib/supabase/server';

// Helper to validate frequency
const isValidFrequency = (freq: string) => ['daily', 'weekly'].includes(freq);

// GET: List scheduled scrapes
export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient();
    const { data: { user }, error: authError } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { data: schedules, error: dbError } = await supabase
      .from('scheduled_scrapes')
      .select('*')
      .eq('user_id', user.id)
      .order('created_at', { ascending: false });

    if (dbError) throw dbError;

    return NextResponse.json({ schedules });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

// POST: Create a new schedule
export async function POST(request: NextRequest) {
  try {
    const supabase = await createClient();
    const { data: { user }, error: authError } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { platform, url, frequency } = await request.json();

    if (!platform || !url || !frequency) {
        return NextResponse.json({ error: 'Missing Required Fields' }, { status: 400 });
    }

    if (!isValidFrequency(frequency)) {
        return NextResponse.json({ error: 'Invalid Frequency' }, { status: 400 });
    }
    
    // Calculate next run at 6:00 AM IST (00:30 UTC)
    // IST is UTC+5:30, so 6:00 AM IST = 00:30 UTC
    const now = new Date();
    const nextRun = new Date(now);
    
    // Set to tomorrow (or +7 days for weekly) at 6:00 AM IST
    if (frequency === 'daily') {
      nextRun.setDate(now.getDate() + 1);
    } else {
      nextRun.setDate(now.getDate() + 7);
    }
    
    // Set time to 00:30 UTC (6:00 AM IST)
    nextRun.setUTCHours(0, 30, 0, 0);

    const { data, error } = await supabase
      .from('scheduled_scrapes')
      .insert({
        user_id: user.id,
        platform,
        target_url: url,
        frequency,
        next_run_at: nextRun.toISOString()
      })
      .select()
      .single();

    if (error) throw error;

    return NextResponse.json({ schedule: data });
  } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
  }
}

// DELETE: Cancel a schedule
export async function DELETE(request: NextRequest) {
   try {
    const supabase = await createClient();
    const { data: { user }, error: authError } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const { searchParams } = new URL(request.url);
    const id = searchParams.get('id');

    if (!id) {
        return NextResponse.json({ error: 'Missing ID' }, { status: 400 });
    }

    const { error } = await supabase
        .from('scheduled_scrapes')
        .delete()
        .eq('id', id)
        .eq('user_id', user.id); // Security: Ensure ownership

    if (error) throw error;

    return NextResponse.json({ success: true });
   } catch (error: any) {
    return NextResponse.json({ error: error.message }, { status: 500 });
   }
}

