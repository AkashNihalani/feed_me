import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { privateJsonResponse } from '@/lib/privateJsonResponse';

type DbPatternRow = {
  id: number;
  feeder_file_id: number;
  feeder_handle: string;
  pattern_id: string;
  status: string;
  core_post_count: number | null;
  support_post_count: number | null;
  pattern_read: Record<string, unknown> | null;
  proof_reads: Array<Record<string, unknown>> | null;
  updated_at: string | null;
};

type DbFeederFileRow = {
  id: number;
  feed_file: Record<string, unknown> | null;
};

type ApiMetric = {
  label: string;
  value: string;
  detail: string;
  accent?: boolean;
};

type ApiProof = {
  post_key: string;
  post_url: string | null;
  proof_label: string;
  proof_headline: string;
  post_read: string;
  what_clicked: string;
  evidence: string[];
  metrics: ApiMetric[];
};

type ApiPattern = {
  account: string;
  accountLabel: string;
  accountMeta: string;
  accountMemoryMeta?: string;
  pattern_id: string;
  patternMetrics: ApiMetric[];
  pattern: {
    pattern_id: string;
    tile_label: string;
    tile_headline: string;
    tile_read: string;
    modal_headline: string;
    the_hook: string;
    the_breakdown: string[];
    why_it_works: string;
    what_to_keep: string[];
    what_kills_it: string[];
  };
  proofs: ApiProof[];
};

type PostUrlRow = {
  post_key: string | null;
  post_url: string | null;
};

const TEMP_LOCAL_FEEDER_FILE_HANDLES = process.env.NODE_ENV === 'development'
  ? ['traya.health']
  : [];

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

function normalizeHandle(value: string | null | undefined) {
  return String(value || '').trim().replace(/^@+/, '').toLowerCase();
}

function accountForHandle(value: string | null | undefined) {
  const handle = normalizeHandle(value);
  return handle ? `@${handle}` : '';
}

function text(value: unknown, fallback = '') {
  return typeof value === 'string' ? value : fallback;
}

function objectValue(value: unknown) {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {};
}

function numberValue(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function accountMemoryMeta(feedFile: Record<string, unknown> | null | undefined) {
  const activeMemory = objectValue(feedFile?.active_post_memory);
  const rankedSlots = objectValue(activeMemory.ranked_winner_slots);
  const target = numberValue(rankedSlots.target);
  const rankedWinners = numberValue(rankedSlots.filled_by_ranked_winners);
  const recentFill = numberValue(rankedSlots.filled_by_recent_fill);
  if (target > 0 && (rankedWinners < target || recentFill > 0)) {
    return 'Still learning this account. Recent posts are carrying more of the read right now.';
  }
  if (target > 0 && rankedWinners >= target) {
    return 'Enough history now. Feed Me has a clearer memory of what this account wins with.';
  }
  return '';
}

function stringList(value: unknown) {
  return Array.isArray(value) ? value.map((item) => String(item || '').trim()).filter(Boolean) : [];
}

function metricList(value: unknown) {
  return Array.isArray(value)
    ? value
        .filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object' && !Array.isArray(item)))
        .map((item) => ({
          label: text(item.label, 'Metric'),
          value: text(item.value, ''),
          detail: text(item.detail, ''),
          accent: Boolean(item.accent),
        }))
    : [];
}

function proofList(value: unknown) {
  return Array.isArray(value)
    ? value.filter((item): item is Record<string, unknown> => Boolean(item && typeof item === 'object' && !Array.isArray(item)))
    : [];
}

function proofKey(value: unknown) {
  return String(value || '').trim().toLowerCase();
}

function postUrlForProof(postUrlByKey: Map<string, string>, postKey: string) {
  const exact = String(postKey || '').trim();
  if (!exact) return null;
  return postUrlByKey.get(exact) || postUrlByKey.get(exact.toLowerCase()) || null;
}

function reindexProofs(proofs: ApiProof[]): ApiProof[] {
  const total = proofs.length;
  return proofs.map((proof, index) => ({
    ...proof,
    metrics: proof.metrics.map((metric) => (
      metric.label.toLowerCase() === 'proof'
        ? { ...metric, value: `${index + 1}/${total}` }
        : metric
    )),
  }));
}

function updateProofMetric(pattern: ApiPattern, proofs: ApiProof[]): ApiPattern {
  return {
    ...pattern,
    patternMetrics: pattern.patternMetrics.map((metric) => (
      metric.label.toLowerCase() === 'proofs'
        ? { ...metric, value: String(proofs.length) }
        : metric
    )),
    proofs: reindexProofs(proofs),
  };
}

function withUniquePostProofs(patterns: ApiPattern[]): ApiPattern[] {
  const claimedPostKeys = new Set<string>();
  return patterns.map((pattern) => {
    const uniqueProofs: ApiProof[] = [];
    for (const proof of pattern.proofs) {
      const key = proofKey(proof.post_key);
      if (!key) continue;
      if (claimedPostKeys.has(key)) continue;
      claimedPostKeys.add(key);
      uniqueProofs.push(proof);
    }
    return updateProofMetric(pattern, uniqueProofs);
  });
}

export async function GET(req: NextRequest) {
  const sb = await createClient();
  const { data: { user } } = await sb.auth.getUser();
  if (!user) return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });

  const url = new URL(req.url);
  const feedId = Number(url.searchParams.get('feedId') || 0);
  const requestedHandle = normalizeHandle(url.searchParams.get('handle'));
  const selectedHandle = requestedHandle === 'all' ? '' : requestedHandle;
  if (!feedId) return NextResponse.json({ error: 'feedId is required' }, { status: 400 });

  const { data: feed, error: feedError } = await sb
    .from('feeds')
    .select('id')
    .eq('id', feedId)
    .eq('user_id', user.id)
    .maybeSingle();
  if (feedError) throw feedError;
  if (!feed) return NextResponse.json({ error: 'Feed not found' }, { status: 404 });

  const { data: ownedFeeds, error: ownedFeedsError } = await sb
    .from('feeds')
    .select('id')
    .eq('user_id', user.id);
  if (ownedFeedsError) throw ownedFeedsError;

  const ownedFeedIds = (ownedFeeds || [])
    .map((row) => Number(row.id))
    .filter((id) => Number.isFinite(id));
  if (ownedFeedIds.length === 0) {
    return privateJsonResponse(req, { patterns: [] });
  }

  const { data: feederRows, error: feederError } = await sb
    .from('feeders')
    .select('handle')
    .in('feed_id', ownedFeedIds)
    .eq('status', 'active');
  if (feederError) throw feederError;

  const handleSet = new Set((feederRows || [])
    .map((row) => normalizeHandle(row.handle))
    .filter(Boolean));
  for (const handle of TEMP_LOCAL_FEEDER_FILE_HANDLES) {
    handleSet.add(normalizeHandle(handle));
  }
  const handles = Array.from(handleSet)
    .filter((handle) => !selectedHandle || handle === selectedHandle);

  if (handles.length === 0) {
    return privateJsonResponse(req, { patterns: [] });
  }

  const admin = adminClient();
  const { data: patternRows, error: patternError } = await admin
    .from('feeder_file_patterns')
    .select('id,feeder_file_id,feeder_handle,pattern_id,status,core_post_count,support_post_count,pattern_read,proof_reads,updated_at')
    .in('feeder_handle', handles)
    .eq('status', 'active')
    .order('updated_at', { ascending: false })
    .limit(48);
  if (patternError) throw patternError;

  const uniquePatterns = new Map<string, DbPatternRow>();
  for (const row of (patternRows || []) as DbPatternRow[]) {
    if (!row.pattern_read || Object.keys(row.pattern_read).length === 0) continue;
    const key = `${normalizeHandle(row.feeder_handle)}:${row.pattern_id}`;
    if (!uniquePatterns.has(key)) uniquePatterns.set(key, row);
  }

  const feederFileIds = Array.from(new Set(
    Array.from(uniquePatterns.values())
      .map((pattern) => Number(pattern.feeder_file_id))
      .filter((id) => Number.isFinite(id)),
  ));
  const feedFileById = new Map<number, Record<string, unknown>>();
  if (feederFileIds.length > 0) {
    const { data: feederFileRows, error: feederFileError } = await admin
      .from('feeder_files')
      .select('id,feed_file')
      .in('id', feederFileIds);
    if (feederFileError) throw feederFileError;
    for (const row of (feederFileRows || []) as DbFeederFileRow[]) {
      if (row.feed_file) feedFileById.set(Number(row.id), row.feed_file);
    }
  }

  const proofPostKeys = Array.from(new Set(
    Array.from(uniquePatterns.values())
      .flatMap((pattern) => proofList(pattern.proof_reads).map((proof) => text(proof.post_key)))
      .filter(Boolean),
  ));
  const postUrlByKey = new Map<string, string>();
  if (proofPostKeys.length > 0) {
    const { data: postUrlRows, error: postUrlError } = await admin
      .from('posts')
      .select('post_key,post_url')
      .in('post_key', proofPostKeys);
    if (postUrlError) throw postUrlError;
    for (const row of (postUrlRows || []) as PostUrlRow[]) {
      const key = text(row.post_key);
      const urlValue = text(row.post_url);
      if (!key || !urlValue) continue;
      postUrlByKey.set(key, urlValue);
      postUrlByKey.set(key.toLowerCase(), urlValue);
    }
  }

  const patterns = Array.from(uniquePatterns.values())
    .sort((a, b) => {
      const handleCompare = normalizeHandle(a.feeder_handle).localeCompare(normalizeHandle(b.feeder_handle));
      if (handleCompare !== 0) return handleCompare;
      return String(a.pattern_id || '').localeCompare(String(b.pattern_id || ''));
    })
    .map<ApiPattern>((pattern) => {
      const patternRead = pattern.pattern_read || {};
      const proofs = proofList(pattern.proof_reads);
      const account = accountForHandle(pattern.feeder_handle);
      const memoryMeta = accountMemoryMeta(feedFileById.get(Number(pattern.feeder_file_id)));
      return {
        account,
        accountLabel: account,
        accountMeta: `${Number(pattern.core_post_count || 0)} core posts · ${Number(pattern.support_post_count || 0)} support posts`,
        ...(memoryMeta ? { accountMemoryMeta: memoryMeta } : {}),
        pattern_id: pattern.pattern_id,
        patternMetrics: [
          { label: 'Proofs', value: String(proofs.length), detail: 'post proof reads', accent: true },
          { label: 'Core', value: String(pattern.core_post_count || 0), detail: 'pattern members' },
          { label: 'Support', value: String(pattern.support_post_count || 0), detail: 'nearby proof' },
        ],
        pattern: {
          pattern_id: pattern.pattern_id,
          tile_label: text(patternRead.tile_label || patternRead.headline, 'Pattern'),
          tile_headline: text(patternRead.tile_headline || patternRead.headline, 'Pattern read'),
          tile_read: text(patternRead.tile_read || patternRead.the_hook),
          modal_headline: text(patternRead.modal_headline || patternRead.headline, 'Pattern read'),
          the_hook: text(patternRead.the_hook),
          the_breakdown: stringList(patternRead.the_breakdown),
          why_it_works: text(patternRead.why_it_works),
          what_to_keep: stringList(patternRead.what_to_keep),
          what_kills_it: stringList(patternRead.what_kills_it),
        },
        proofs: proofs.map((proof, index) => ({
          post_key: text(proof.post_key),
          post_url: text(proof.post_url) || postUrlForProof(postUrlByKey, text(proof.post_key)),
          proof_label: text(proof.proof_label, `Proof ${index + 1}`),
          proof_headline: text(proof.proof_headline),
          post_read: text(proof.post_read),
          what_clicked: text(proof.what_clicked),
          evidence: stringList(proof.evidence),
          metrics: metricList(proof.metrics),
        })),
      };
    });

  return privateJsonResponse(req, {
    patterns: withUniquePostProofs(patterns),
  });
}
