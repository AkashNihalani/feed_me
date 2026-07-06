export const CANONICAL_FIRE_METRICS = ['engagement_rate', 'likes', 'comments'] as const;
export type FireMetricKey = (typeof CANONICAL_FIRE_METRICS)[number];

type MetricPayloadMap = Record<string, unknown>;

type MetricPayloadValue = {
  key: FireMetricKey;
  value: number | null;
  multiple: number | null;
  baseline: number | null;
};

function asRecord(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function metricPayload(metrics: MetricPayloadMap, metric: FireMetricKey): Record<string, unknown> {
  return asRecord(metrics[metric]);
}

export function metricValueFromPayload(metrics: MetricPayloadMap, metric: FireMetricKey): number | null {
  return asNumber(metricPayload(metrics, metric).value);
}

export function metricMultipleFromPayload(metrics: MetricPayloadMap, metric: FireMetricKey): number | null {
  return asNumber(metricPayload(metrics, metric).multiple);
}

export function metricBaselineFromPayload(metrics: MetricPayloadMap, metric: FireMetricKey): number | null {
  return asNumber(metricPayload(metrics, metric).baseline);
}

export function metricLabel(metric: FireMetricKey, variant: 'plural' | 'singular' | 'short' = 'plural'): string {
  if (metric === 'engagement_rate') return variant === 'short' ? 'ER' : 'Engagement Rate';
  if (variant === 'singular') {
    if (metric === 'likes') return 'Like';
    return 'Comment';
  }
  if (metric === 'likes') return 'Likes';
  return 'Comments';
}

function compactCount(v: number): string {
  const n = Math.abs(v);
  if (n >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(2)}B`;
  if (n >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return String(Math.round(v));
}

function formatEngagementRate(value: number): string {
  const pct = Math.abs(value) <= 1 ? value * 100 : value;
  const decimals = Math.abs(pct) >= 10 ? 1 : 2;
  return `${pct.toFixed(decimals).replace(/\.?0+$/, '')}%`;
}

export function formatMetricValue(metric: FireMetricKey, value: number | null, missing = 'INSUFFICIENT DATA'): string {
  if (value == null || !Number.isFinite(value)) return missing;
  return metric === 'engagement_rate' ? formatEngagementRate(value) : compactCount(value);
}

export function resolveBestMetricFromPayload(
  metrics: MetricPayloadMap,
  preferred: string,
): FireMetricKey {
  const normalizedPreferred = preferred.trim().toLowerCase();
  if (
    (normalizedPreferred === 'engagement_rate' || normalizedPreferred === 'likes' || normalizedPreferred === 'comments')
    && metricValueFromPayload(metrics, normalizedPreferred) != null
  ) {
    return normalizedPreferred;
  }

  const ordered = CANONICAL_FIRE_METRICS;
  let bestMetric: FireMetricKey | null = null;
  let bestMultiple: number | null = null;

  for (const metric of ordered) {
    const multiple = metricMultipleFromPayload(metrics, metric);
    if (multiple == null) continue;
    if (bestMultiple == null || multiple > bestMultiple) {
      bestMetric = metric;
      bestMultiple = multiple;
    }
  }

  if (bestMetric) return bestMetric;

  for (const metric of ordered) {
    if (metricValueFromPayload(metrics, metric) != null) return metric;
  }

  return ordered[0];
}

function compareMetricPayloadValues(
  a: MetricPayloadValue,
  b: MetricPayloadValue,
): number {
  const aMultiple = a.multiple ?? Number.NEGATIVE_INFINITY;
  const bMultiple = b.multiple ?? Number.NEGATIVE_INFINITY;
  if (aMultiple !== bMultiple) return bMultiple - aMultiple;

  const aValue = a.value ?? Number.NEGATIVE_INFINITY;
  const bValue = b.value ?? Number.NEGATIVE_INFINITY;
  if (aValue !== bValue) return bValue - aValue;

  const order = CANONICAL_FIRE_METRICS;
  return order.indexOf(a.key) - order.indexOf(b.key);
}

export function orderedSupportMetricsFromPayload(
  metrics: MetricPayloadMap,
  bestMetric: FireMetricKey,
): MetricPayloadValue[] {
  return CANONICAL_FIRE_METRICS
    .filter((metric) => metric !== bestMetric)
    .map((metric) => ({
      key: metric,
      value: metricValueFromPayload(metrics, metric),
      multiple: metricMultipleFromPayload(metrics, metric),
      baseline: metricBaselineFromPayload(metrics, metric),
    }))
    .sort(compareMetricPayloadValues);
}
