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

type FeedIdRow = { id: number };
type FeederRow = { id: number; handle: string | null; feed_id: number; status: string };
type RunJobRow = {
  id: number;
  feeder_id: number;
  job_type: string | null;
  status: string;
  attempt?: number | null;
  created_at?: string | null;
  updated_at: string;
  next_run_at?: string | null;
  last_error?: string | null;
};
type RunJobStatusRow = { status: string | null };
type PostRow = { post_key: string; feeder_id: number; media_type: string | null; posted_at: string | null };
type CheckpointJobRow = {
  id: number;
  post_key: string;
  checkpoint: string | null;
  status: string;
  next_run_at: string | null;
  updated_at: string;
};
type FireAlertRow = {
  id: number;
  feeder_id: number | null;
  post_key: string;
  checkpoint: string | null;
  created_at: string;
  business_date_ist: string | null;
};
type RecentJob = {
  id: number;
  handle: string;
  jobType: string | null;
  status: string;
  attempt: number | null;
  createdAt: string | null;
  updatedAt: string;
  lastError: string | null;
};

const FIRE_DROP_READY_BUFFER_MS = 3 * 60 * 1000;
const FIRE_DROP_LIVE_HOLD_MS = 6 * 60 * 1000;

function toIstDayKey(date: Date): string {
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function checkpointBusinessDay(postedAt: string | null | undefined, checkpoint: string | null | undefined): string | null {
  if (!postedAt || !checkpoint) return null;
  const base = new Date(postedAt);
  if (Number.isNaN(base.getTime())) return null;

  const cp = checkpoint.toLowerCase();
  const daysAfter = cp === 'd1' ? 1 : cp === 'd3' ? 3 : cp === 'd7' ? 7 : cp === 'd21' ? 21 : null;
  if (daysAfter == null) return null;

  base.setTime(base.getTime() + daysAfter * 24 * 60 * 60 * 1000);
  return toIstDayKey(base);
}

function todayIstDayKey(): string {
  return toIstDayKey(new Date());
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
    const feedRows = (feeds || []) as FeedIdRow[];
    const feedIds = feedRows.map((f) => f.id);

    if (feedIds.length === 0) {
      return NextResponse.json({
        recentJobs: [],
        totalFeeders: 0,
        totalPosts: 0,
        jobStats: { done: 0, failed: 0, pending: 0, running: 0 },
        queuedBatches: [],
        completedBatches: [],
        queuedRuns: [],
        completedRuns: [],
      });
    }

    // Get feeders for this user's feeds
    const { data: feeders } = await sb
      .from('feeders')
      .select('id,handle,feed_id,status')
      .in('feed_id', feedIds);
    const feederRows = (feeders || []) as FeederRow[];
    const activeFeeders = feederRows.filter((f) => f.status === 'active');
    const feederIds = activeFeeders.map((f) => f.id);
    const feederMap = new Map(activeFeeders.map((f) => [f.id, f.handle]));

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
    let recentJobs: RecentJob[] = [];
    if (feederIds.length > 0) {
      const { data: jobs } = await sb
        .from('run_jobs')
        .select('id,feeder_id,job_type,status,attempt,created_at,updated_at,last_error')
        .in('feeder_id', feederIds)
        .order('updated_at', { ascending: false })
        .limit(20);

      const jobRows = (jobs || []) as RunJobRow[];
      recentJobs = jobRows.map((j) => ({
        id: j.id,
        handle: feederMap.get(j.feeder_id) || 'unknown',
        jobType: j.job_type,
        status: j.status,
        attempt: j.attempt ?? null,
        createdAt: j.created_at ?? null,
        updatedAt: j.updated_at,
        lastError: j.last_error ?? null,
      }));
    }

    // Job status counts (last 7 days)
    const jobStats = { done: 0, failed: 0, pending: 0, running: 0 };
    if (feederIds.length > 0) {
      const sevenDaysAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString();
      const { data: allJobs } = await sb
        .from('run_jobs')
        .select('status')
        .in('feeder_id', feederIds)
        .gte('updated_at', sevenDaysAgo);

      const jobStatusRows = (allJobs || []) as RunJobStatusRow[];
      for (const j of jobStatusRows) {
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
      businessDay?: string;
      mediaType?: string;
      handle?: string;
      status: string;
      scheduledAt: string;
      completedAt?: string;
    };

    type RunBatchBreakdown = {
      label: string;
      count: number;
    };

    type RunBatch = {
      id: string;
      headline: string;
      windowStart: string;
      windowEnd: string;
      totalRuns: number;
      creatorCount: number;
      systemCount: number;
      businessStart: string | null;
      businessEnd: string | null;
      types: RunBatchBreakdown[];
      statuses: RunBatchBreakdown[];
    };

    const CHECKPOINT_LABEL_ORDER = ['D1', 'D3', 'D7', 'D21'];
    const RUN_LABEL_ORDER = ['DAILY', 'POLL', 'FOLLOWERS', 'REPAIR'];
    const STATUS_ORDER = ['RUNNING', 'PENDING', 'RETRY', 'DONE', 'FAILED', 'SKIPPED'];

    function istHourBucketStartIso(value: string): string | null {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return null;

      const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Kolkata',
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        hour12: false,
      }).formatToParts(date);

      const lookup = new Map(parts.map((part) => [part.type, part.value]));
      const year = Number.parseInt(lookup.get('year') || '', 10);
      const month = Number.parseInt(lookup.get('month') || '', 10);
      const day = Number.parseInt(lookup.get('day') || '', 10);
      const hour = Number.parseInt(lookup.get('hour') || '', 10);
      if (![year, month, day, hour].every(Number.isFinite)) return null;

      const utcMillis = Date.UTC(year, month - 1, day, hour, 0, 0, 0) - (5.5 * 60 * 60 * 1000);
      return new Date(utcMillis).toISOString();
    }

    function sortBreakdown(items: Map<string, number>, order: string[]): RunBatchBreakdown[] {
      return Array.from(items.entries())
        .map(([label, count]) => ({ label, count }))
        .sort((a, b) => {
          const ai = order.indexOf(a.label);
          const bi = order.indexOf(b.label);
          if (ai !== -1 || bi !== -1) {
            if (ai === -1) return 1;
            if (bi === -1) return -1;
            if (ai !== bi) return ai - bi;
          }
          if (b.count !== a.count) return b.count - a.count;
          return a.label.localeCompare(b.label);
        });
    }

    function batchHeadline(): string {
      return 'Fire Update';
    }

    function summarizeRunsIntoBatches(
      runs: UnifiedRun[],
      mode: 'queued' | 'completed',
      limit = 5,
    ): RunBatch[] {
      const buckets = new Map<string, {
        windowStart: string;
        totalRuns: number;
        handles: Set<string>;
        systemCount: number;
        businessDays: Set<string>;
        types: Map<string, number>;
        statuses: Map<string, number>;
        firstStamp: number;
        lastStamp: number;
      }>();

      for (const run of runs) {
        const stamp = mode === 'queued' ? run.scheduledAt : (run.completedAt || run.scheduledAt);
        const bucketStart = istHourBucketStartIso(stamp);
        if (!bucketStart) continue;

        const bucket = buckets.get(bucketStart) || {
          windowStart: bucketStart,
          totalRuns: 0,
          handles: new Set<string>(),
          systemCount: 0,
          businessDays: new Set<string>(),
          types: new Map<string, number>(),
          statuses: new Map<string, number>(),
          firstStamp: Number.POSITIVE_INFINITY,
          lastStamp: Number.NEGATIVE_INFINITY,
        };

        const stampTs = new Date(stamp).getTime();
        bucket.totalRuns += 1;
        if (run.handle) bucket.handles.add(run.handle);
        else bucket.systemCount += 1;
        if (run.businessDay) bucket.businessDays.add(run.businessDay);
        if (Number.isFinite(stampTs)) {
          bucket.firstStamp = Math.min(bucket.firstStamp, stampTs);
          bucket.lastStamp = Math.max(bucket.lastStamp, stampTs);
        }

        const typeLabel = (run.checkpoint || run.label || run.kind || '?').toUpperCase();
        bucket.types.set(typeLabel, (bucket.types.get(typeLabel) || 0) + 1);

        const statusLabel = (run.status || '?').toUpperCase();
        bucket.statuses.set(statusLabel, (bucket.statuses.get(statusLabel) || 0) + 1);

        buckets.set(bucketStart, bucket);
      }

      return Array.from(buckets.values())
        .sort((a, b) => {
          const aTs = new Date(a.windowStart).getTime();
          const bTs = new Date(b.windowStart).getTime();
          return mode === 'queued' ? aTs - bTs : bTs - aTs;
        })
        .slice(0, limit)
        .map((bucket) => {
          const orderedBusinessDays = Array.from(bucket.businessDays).sort((a, b) => a.localeCompare(b));
          const firstStampIso = Number.isFinite(bucket.firstStamp)
            ? new Date(bucket.firstStamp).toISOString()
            : bucket.windowStart;
          const lastStampIso = Number.isFinite(bucket.lastStamp)
            ? new Date(bucket.lastStamp).toISOString()
            : bucket.windowStart;
          return {
            id: `${mode}-${bucket.windowStart}`,
            headline: batchHeadline(),
            windowStart: firstStampIso,
            windowEnd: lastStampIso,
            totalRuns: bucket.totalRuns,
            creatorCount: bucket.handles.size,
            systemCount: bucket.systemCount,
            businessStart: orderedBusinessDays[0] || null,
            businessEnd: orderedBusinessDays[orderedBusinessDays.length - 1] || null,
            types: sortBreakdown(bucket.types, [...CHECKPOINT_LABEL_ORDER, ...RUN_LABEL_ORDER]),
            statuses: sortBreakdown(bucket.statuses, STATUS_ORDER),
          };
        });
    }

    function fireReadyAtIso(value: string): string | null {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return null;
      return new Date(date.getTime() + FIRE_DROP_READY_BUFFER_MS).toISOString();
    }

    function fireDisplayFloor(now = new Date()): number {
      return now.getTime() - FIRE_DROP_LIVE_HOLD_MS;
    }

    function isDisplayableQueuedRun(run: UnifiedRun, floorTs: number): boolean {
      const scheduledTs = new Date(run.scheduledAt).getTime();
      if (!Number.isFinite(scheduledTs)) return true;
      return scheduledTs >= floorTs;
    }

    function isUserFacingCheckpointRun(run: UnifiedRun, currentFireDay: string): boolean {
      if (run.kind !== 'CHECKPOINT') return true;
      if (!run.businessDay) return false;
      return run.businessDay >= currentFireDay;
    }

    let queuedRuns: UnifiedRun[] = [];
    let completedRuns: UnifiedRun[] = [];
    let queuedBatches: RunBatch[] = [];
    let completedBatches: RunBatch[] = [];

    if (feederIds.length > 0) {
      const RUN_LIMIT = 250; // keep enough rows to summarize Fire-facing timing accurately
      const queueFloorTs = fireDisplayFloor();
      const currentFireDay = todayIstDayKey();

      // Get all post_keys for this user's feeders to scope checkpoint queries
      const { data: postRows } = await sb
        .from('posts')
        .select('post_key,feeder_id,media_type,posted_at')
        .in('feeder_id', feederIds);
      const scopedPosts = (postRows || []) as PostRow[];
      const postMap = new Map(scopedPosts.map((p) => [p.post_key, p]));
      const postKeys = Array.from(postMap.keys());

      let queuedCpJobs: CheckpointJobRow[] = [];
      let recentFireAlerts: FireAlertRow[] = [];

      if (postKeys.length > 0) {
        const { data: qCp } = await sb
          .from('checkpoint_jobs')
          .select('id,post_key,checkpoint,status,next_run_at,updated_at')
          .in('post_key', postKeys)
          .in('status', ['pending', 'retry', 'running'])
          .order('next_run_at', { ascending: true })
          .limit(RUN_LIMIT);
        queuedCpJobs = (qCp || []) as CheckpointJobRow[];

        const { data: fireAlerts } = await sb
          .from('fire_alerts')
          .select('id,feeder_id,post_key,checkpoint,created_at,business_date_ist')
          .in('feeder_id', feederIds)
          .order('created_at', { ascending: false })
          .limit(RUN_LIMIT);
        recentFireAlerts = (fireAlerts || []) as FireAlertRow[];
      }

      const normalizeQueuedCpJob = (j: CheckpointJobRow): UnifiedRun => {
        const post = postMap.get(j.post_key);
        const fireReadyAt = fireReadyAtIso(j.next_run_at || j.updated_at) || j.next_run_at || j.updated_at;
        return {
          id: `cp-${j.id}`,
          kind: 'CHECKPOINT',
          label: (j.checkpoint || '').toUpperCase(),
          checkpoint: (j.checkpoint || '').toUpperCase(),
          businessDay: checkpointBusinessDay(post?.posted_at || null, j.checkpoint) || undefined,
          mediaType: post?.media_type || undefined,
          handle: post ? (feederMap.get(post.feeder_id) || undefined) : undefined,
          status: j.status,
          scheduledAt: fireReadyAt,
        };
      };

      const normalizeFireAlert = (row: FireAlertRow): UnifiedRun => {
        const post = postMap.get(row.post_key);
        return {
          id: `fire-${row.id}`,
          kind: 'CHECKPOINT',
          label: (row.checkpoint || '').toUpperCase(),
          checkpoint: (row.checkpoint || '').toUpperCase(),
          businessDay: row.business_date_ist || checkpointBusinessDay(post?.posted_at || null, row.checkpoint) || undefined,
          mediaType: post?.media_type || undefined,
          handle: row.feeder_id != null ? (feederMap.get(row.feeder_id) || undefined) : undefined,
          status: 'live',
          scheduledAt: row.created_at,
          completedAt: row.created_at,
        };
      };

      queuedRuns = queuedCpJobs
        .map(normalizeQueuedCpJob)
        .filter((run) => isUserFacingCheckpointRun(run, currentFireDay))
        .filter((run) => isDisplayableQueuedRun(run, queueFloorTs))
        .sort((a, b) => new Date(a.scheduledAt).getTime() - new Date(b.scheduledAt).getTime())
        .slice(0, RUN_LIMIT);

      completedRuns = recentFireAlerts
        .map(normalizeFireAlert)
        .sort((a, b) => new Date(b.completedAt || b.scheduledAt).getTime() - new Date(a.completedAt || a.scheduledAt).getTime())
        .slice(0, RUN_LIMIT);

      queuedBatches = summarizeRunsIntoBatches(queuedRuns, 'queued');
      completedBatches = summarizeRunsIntoBatches(completedRuns, 'completed');
    }

    return NextResponse.json({
      recentJobs,
      totalFeeders: activeFeeders.length,
      totalPosts,
      jobStats,
      queuedBatches,
      completedBatches,
      queuedRuns,
      completedRuns,
    });
  } catch (error: unknown) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'Failed to load engine stats' },
      { status: 500 }
    );
  }
}
