'use client';

import { FireItem } from './types';
import { getPatternCueLabel, getPatternMechanicLabel } from '@/lib/fireSignals';

type UnknownRecord = Record<string, unknown>;

function asRecord(value: unknown): UnknownRecord {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as UnknownRecord) : {};
}

function asString(value: unknown): string {
  if (typeof value === 'string') return value;
  if (typeof value === 'number') return String(value);
  return '';
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function uniqueByPostKey<T extends { postKey: string }>(items: T[]): T[] {
  const seen = new Set<string>();
  const result: T[] = [];
  for (const item of items) {
    if (!item.postKey || seen.has(item.postKey)) continue;
    seen.add(item.postKey);
    result.push(item);
  }
  return result;
}

export type FirewatchPostPreview = {
  postKey: string;
  handle: string;
  mediaType: string;
  postUrl: string;
  thumbnailUrl: string;
};

export type FirewatchData = {
  feedName: string;
  familyLabel: string;
  patternLabel: string;
  mediaType: string;
  matchCount: number | null;
  feedersCount: number | null;
  avgHotPercentile: number | null;
  baselineShare: number | null;
  recentLift: number | null;
  anchorGap: number | null;
  anchorAvgPercentile: number | null;
  cues: string[];
  requiredCues: string[];
  supportPosts: FirewatchPostPreview[];
  anchorSupportPosts: FirewatchPostPreview[];
  coverPosts: FirewatchPostPreview[];
};

export function parseFirewatchData(item: FireItem): FirewatchData {
  const payload = asRecord(item.payload);
  const meta = asRecord(payload.meta);
  const firewatch = asRecord(meta.firewatch);

  const mapPosts = (value: unknown): FirewatchPostPreview[] => (
    Array.isArray(value)
      ? value
        .map((post) => asRecord(post))
        .map((post) => ({
          postKey: asString(post.post_key),
          handle: asString(post.handle),
          mediaType: asString(post.media_type).toUpperCase(),
          postUrl: asString(post.post_url),
          thumbnailUrl: asString(post.thumbnail_url),
        }))
        .filter((post) => post.postKey && post.postUrl && post.thumbnailUrl)
      : []
  );

  const mapCueLabels = (value: unknown): string[] => (
    Array.isArray(value)
      ? value
        .map((cue) => asRecord(cue))
        .map((cue) => asString(cue.label) || getPatternCueLabel(asString(cue.key), asString(cue.value)) || '')
        .filter(Boolean)
      : []
  );

  const supportPosts = mapPosts(firewatch.support_posts);
  const anchorSupportPosts = mapPosts(firewatch.anchor_support_posts);
  const matchingSupportHero = supportPosts.find(
    (post) => post.thumbnailUrl === item.thumbnailUrl && post.postUrl === item.postUrl,
  );
  const heroPost = item.thumbnailUrl && item.postUrl
    ? [{
        postKey: item.postKey || matchingSupportHero?.postKey || `hero:${item.id}`,
        handle: item.surfaceHandle,
        mediaType: (item.surfaceMediaType || item.mediaType || '').toUpperCase(),
        postUrl: item.postUrl,
        thumbnailUrl: item.thumbnailUrl,
      }]
    : [];
  const coverPosts = uniqueByPostKey([...heroPost, ...supportPosts]).slice(0, 4);

  return {
    feedName: asString(meta.feed_name) || item.handle || '@FEED',
    familyLabel: asString(firewatch.family_label) || 'Firewatch',
    patternLabel: asString(firewatch.pattern_label)
      || getPatternMechanicLabel(asString(firewatch.pattern_name))
      || item.signalHeadline
      || 'Pattern alert',
    mediaType: asString(firewatch.media_type).toUpperCase() || item.surfaceMediaType || item.mediaType || 'POST',
    matchCount: asNumber(firewatch.match_count),
    feedersCount: asNumber(firewatch.feeders_count),
    avgHotPercentile: asNumber(firewatch.avg_hot_percentile) ?? item.surfacePercentileExact ?? item.surfacePercentile,
    baselineShare: asNumber(firewatch.baseline_share),
    recentLift: asNumber(firewatch.recent_lift),
    anchorGap: asNumber(firewatch.anchor_gap),
    anchorAvgPercentile: asNumber(firewatch.anchor_avg_percentile),
    cues: mapCueLabels(firewatch.cues),
    requiredCues: mapCueLabels(firewatch.required_cues),
    supportPosts,
    anchorSupportPosts,
    coverPosts,
  };
}
