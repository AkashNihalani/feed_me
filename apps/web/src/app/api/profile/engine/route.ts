import { NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';

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

export async function GET() {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const sb = adminClient();

    // Get user's feeds
    const { data: feeds } = await sb
      .from('feeds')
      .select('id')
      .eq('user_id', user.id);
    const feedIds = (feeds || []).map((f: any) => f.id);

    if (feedIds.length === 0) {
      return NextResponse.json({
        recentJobs: [],
        totalFeeders: 0,
        totalPosts: 0,
        jobStats: { done: 0, failed: 0, pending: 0, running: 0 },
      });
    }

    // Get feeders for this user's feeds
    const { data: feeders } = await sb
      .from('feeders')
      .select('id,handle,feed_id,status')
      .in('feed_id', feedIds);
    const activeFeeders = (feeders || []).filter((f: any) => f.status === 'active');
    const feederIds = activeFeeders.map((f: any) => f.id);
    const feederMap = new Map(activeFeeders.map((f: any) => [f.id, f.handle]));

    // Total posts tracked
    let totalPosts = 0;
    if (feederIds.length > 0) {
      const { count } = await sb
        .from('posts')
        .select('*', { count: 'exact', head: true })
        .in('feeder_id', feederIds);
      totalPosts = count || 0;
    }

    // Recent run jobs (last 20)
    let recentJobs: any[] = [];
    if (feederIds.length > 0) {
      const { data: jobs } = await sb
        .from('run_jobs')
        .select('id,feeder_id,job_type,status,attempt,created_at,updated_at,last_error')
        .in('feeder_id', feederIds)
        .order('updated_at', { ascending: false })
        .limit(20);
      
      recentJobs = (jobs || []).map((j: any) => ({
        id: j.id,
        handle: feederMap.get(j.feeder_id) || 'unknown',
        jobType: j.job_type,
        status: j.status,
        attempt: j.attempt,
        createdAt: j.created_at,
        updatedAt: j.updated_at,
        lastError: j.last_error,
      }));
    }

    // Job status counts (last 7 days)
    let jobStats = { done: 0, failed: 0, pending: 0, running: 0 };
    if (feederIds.length > 0) {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const { data: allJobs } = await sb
        .from('run_jobs')
        .select('status')
        .in('feeder_id', feederIds)
        .gte('updated_at', sevenDaysAgo);
      
      for (const j of allJobs || []) {
        if (j.status === 'done') jobStats.done++;
        else if (j.status === 'failed') jobStats.failed++;
        else if (j.status === 'pending' || j.status === 'retry') jobStats.pending++;
        else if (j.status === 'running') jobStats.running++;
      }
    }

    return NextResponse.json({
      recentJobs,
      totalFeeders: activeFeeders.length,
      totalPosts,
      jobStats,
    });
  } catch (error: any) {
    return NextResponse.json(
      { error: error.message || 'Failed to load engine stats' },
      { status: 500 }
    );
  }
}
