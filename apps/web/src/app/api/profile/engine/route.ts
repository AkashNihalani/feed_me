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
        queuedRuns: [],
        completedRuns: [],
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

    // --- Unified runs: queued + completed from run_jobs + checkpoint_jobs ---
    type UnifiedRun = {
      id: string;
      kind: string;
      label: string;
      checkpoint?: string;
      mediaType?: string;
      handle?: string;
      status: string;
      scheduledAt: string;
      completedAt?: string;
    };

    function takeVisibleRuns(
      runJobs: UnifiedRun[],
      checkpointJobs: UnifiedRun[],
      mode: 'queued' | 'completed',
      limit = 15,
      minRunJobs = 4,
    ): UnifiedRun[] {
      const runSorted = [...runJobs].sort((a, b) => {
        const aTs = new Date((mode === 'queued' ? a.scheduledAt : (a.completedAt || a.scheduledAt))).getTime();
        const bTs = new Date((mode === 'queued' ? b.scheduledAt : (b.completedAt || b.scheduledAt))).getTime();
        return mode === 'queued' ? aTs - bTs : bTs - aTs;
      });
      const cpSorted = [...checkpointJobs].sort((a, b) => {
        const aTs = new Date((mode === 'queued' ? a.scheduledAt : (a.completedAt || a.scheduledAt))).getTime();
        const bTs = new Date((mode === 'queued' ? b.scheduledAt : (b.completedAt || b.scheduledAt))).getTime();
        return mode === 'queued' ? aTs - bTs : bTs - aTs;
      });

      const visible: UnifiedRun[] = [];
      const seen = new Set<string>();
      const reservedRuns = Math.min(minRunJobs, runSorted.length, limit);

      for (const run of runSorted.slice(0, reservedRuns)) {
        visible.push(run);
        seen.add(run.id);
      }

      let runIndex = reservedRuns;
      let cpIndex = 0;
      while (visible.length < limit && (runIndex < runSorted.length || cpIndex < cpSorted.length)) {
        const nextRun = runIndex < runSorted.length ? runSorted[runIndex] : null;
        const nextCp = cpIndex < cpSorted.length ? cpSorted[cpIndex] : null;
        const nextRunTs = nextRun ? new Date(mode === 'queued' ? nextRun.scheduledAt : (nextRun.completedAt || nextRun.scheduledAt)).getTime() : null;
        const nextCpTs = nextCp ? new Date(mode === 'queued' ? nextCp.scheduledAt : (nextCp.completedAt || nextCp.scheduledAt)).getTime() : null;

        let pick: UnifiedRun | null = null;
        if (nextRun && nextRunTs != null && (nextCpTs == null || (mode === 'queued' ? nextRunTs <= nextCpTs : nextRunTs >= nextCpTs))) {
          pick = nextRun;
          runIndex += 1;
        } else if (nextCp) {
          pick = nextCp;
          cpIndex += 1;
        } else {
          break;
        }

        if (pick && !seen.has(pick.id)) {
          visible.push(pick);
          seen.add(pick.id);
        }
      }

      return visible;
    }

    function queuedVisibilityFloor(now = new Date()): number {
      const floor = new Date(now);
      floor.setMinutes(0, 0, 0);
      floor.setHours(floor.getHours() - 1);
      return floor.getTime();
    }

    function isDisplayableQueuedRun(run: UnifiedRun, floorTs: number): boolean {
      if (run.status === 'running') return true;
      const scheduledTs = new Date(run.scheduledAt).getTime();
      if (!Number.isFinite(scheduledTs)) return true;
      return scheduledTs >= floorTs;
    }

    let queuedRuns: UnifiedRun[] = [];
    let completedRuns: UnifiedRun[] = [];

    if (feederIds.length > 0) {
      const RUN_LIMIT = 40; // fetch extra so discovery stays visible after merge
      const queueFloorTs = queuedVisibilityFloor();

      // Map run_jobs.job_type to kind/label
      const runKindMap: Record<string, { kind: string; label: string }> = {
        daily: { kind: 'DISCOVERY', label: 'DAILY' },
        poll: { kind: 'POLL', label: 'POLL' },
        followers: { kind: 'PROFILE', label: 'FOLLOWERS' },
        repair: { kind: 'REPAIR', label: 'REPAIR' },
      };

      // Queued run_jobs
      const { data: queuedRunJobs } = await sb
        .from('run_jobs')
        .select('id,feeder_id,job_type,status,next_run_at,updated_at')
        .in('feeder_id', feederIds)
        .in('status', ['pending', 'retry', 'running'])
        .order('next_run_at', { ascending: true })
        .limit(RUN_LIMIT);

      // Completed run_jobs
      const { data: completedRunJobs } = await sb
        .from('run_jobs')
        .select('id,feeder_id,job_type,status,next_run_at,updated_at')
        .in('feeder_id', feederIds)
        .in('status', ['done', 'failed', 'skipped'])
        .order('updated_at', { ascending: false })
        .limit(RUN_LIMIT);

      // Get all post_keys for this user's feeders to scope checkpoint queries
      const { data: postRows } = await sb
        .from('posts')
        .select('post_key,feeder_id,media_type')
        .in('feeder_id', feederIds);
      const postMap = new Map((postRows || []).map((p: any) => [p.post_key, p]));
      const postKeys = Array.from(postMap.keys());

      let queuedCpJobs: any[] = [];
      let completedCpJobs: any[] = [];

      if (postKeys.length > 0) {
        const { data: qCp } = await sb
          .from('checkpoint_jobs')
          .select('id,post_key,checkpoint,status,next_run_at,updated_at')
          .in('post_key', postKeys)
          .in('status', ['pending', 'retry', 'running'])
          .order('next_run_at', { ascending: true })
          .limit(RUN_LIMIT);
        queuedCpJobs = qCp || [];

        const { data: cCp } = await sb
          .from('checkpoint_jobs')
          .select('id,post_key,checkpoint,status,next_run_at,updated_at')
          .in('post_key', postKeys)
          .in('status', ['done', 'failed', 'skipped'])
          .order('updated_at', { ascending: false })
          .limit(RUN_LIMIT);
        completedCpJobs = cCp || [];
      }

      // Normalize run_jobs
      const normalizeRunJob = (j: any): UnifiedRun => {
        const mapped = runKindMap[j.job_type] || { kind: 'DISCOVERY', label: j.job_type?.toUpperCase() || '?' };
        return {
          id: `run-${j.id}`,
          kind: mapped.kind,
          label: mapped.label,
          handle: feederMap.get(j.feeder_id) || undefined,
          status: j.status,
          scheduledAt: j.next_run_at || j.updated_at,
          completedAt: j.updated_at,
        };
      };

      // Normalize checkpoint_jobs
      const normalizeCpJob = (j: any): UnifiedRun => {
        const post = postMap.get(j.post_key);
        return {
          id: `cp-${j.id}`,
          kind: 'CHECKPOINT',
          label: (j.checkpoint || '').toUpperCase(),
          checkpoint: (j.checkpoint || '').toUpperCase(),
          mediaType: post?.media_type || undefined,
          handle: post ? (feederMap.get(post.feeder_id) || undefined) : undefined,
          status: j.status,
          scheduledAt: j.next_run_at || j.updated_at,
          completedAt: j.updated_at,
        };
      };

      // Merge & sort
      queuedRuns = takeVisibleRuns(
        (queuedRunJobs || []).map(normalizeRunJob).filter((run) => isDisplayableQueuedRun(run, queueFloorTs)),
        queuedCpJobs.map(normalizeCpJob).filter((run) => isDisplayableQueuedRun(run, queueFloorTs)),
        'queued',
      );
      completedRuns = takeVisibleRuns(
        (completedRunJobs || []).map(normalizeRunJob),
        completedCpJobs.map(normalizeCpJob),
        'completed',
      );
    }

    return NextResponse.json({
      recentJobs,
      totalFeeders: activeFeeders.length,
      totalPosts,
      jobStats,
      queuedRuns,
      completedRuns,
    });
  } catch (error: any) {
    return NextResponse.json(
      { error: error.message || 'Failed to load engine stats' },
      { status: 500 }
    );
  }
}
