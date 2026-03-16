import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';

export const dynamic = 'force-dynamic';

function adminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) {
    throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  }
  return createSupabaseClient(url, serviceRole, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}

export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const feedId = Number(request.nextUrl.searchParams.get('feedId') || 0);
    const weeks = Number(request.nextUrl.searchParams.get('weeks') || 4);
    const handleRaw = (request.nextUrl.searchParams.get('handle') || '').trim();
    const handle = handleRaw ? handleRaw.replace(/^@+/, '').toLowerCase() : null;

    if (!feedId) {
      return NextResponse.json({ error: 'feedId is required' }, { status: 400 });
    }
    if (![4, 12, 26, 52].includes(weeks)) {
      return NextResponse.json({ error: 'weeks must be one of 4,12,26,52' }, { status: 400 });
    }

    const sb = adminClient();
    const { data: feedRow, error: feedErr } = await sb
      .from('feeds')
      .select('id,user_id')
      .eq('id', feedId)
      .eq('user_id', user.id)
      .single();
    if (feedErr || !feedRow) {
      return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
    }

    const { data, error } = await sb.rpc('fn_feed_dashboard', {
      p_feed_id: feedId,
      p_weeks: weeks,
      p_handle: handle,
    });
    if (error) throw error;

    return NextResponse.json({ dashboard: data || {} });
  } catch (error: any) {
    return NextResponse.json({ error: error.message || 'Failed to load dashboard' }, { status: 500 });
  }
}

