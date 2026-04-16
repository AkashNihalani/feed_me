export type FireMetricKey = 'views' | 'likes' | 'comments';

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

function isReelLikeMediaType(mediaType: string | null | undefined): boolean {
  const normalized = String(mediaType || '').trim().toLowerCase();
  return normalized === 'reel' || normalized === 'video';
}

function metricPreferenceOrder(mediaType: string | null | undefined): FireMetricKey[] {
  return isReelLikeMediaType(mediaType)
    ? ['views', 'likes', 'comments']
    : ['likes', 'comments', 'views'];
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

export function metricLabel(metric: FireMetricKey, variant: 'plural' | 'singular' = 'plural'): string {
  if (variant === 'singular') {
    if (metric === 'views') return 'View';
    if (metric === 'likes') return 'Like';
    return 'Comment';
  }
  if (metric === 'views') return 'Views';
  if (metric === 'likes') return 'Likes';
  return 'Comments';
}

export function resolveBestMetricFromPayload(
  metrics: MetricPayloadMap,
  preferred: string,
  mediaType: string | null | undefined,
): FireMetricKey {
  const normalizedPreferred = preferred.trim().toLowerCase();
  if (
    (normalizedPreferred === 'views' || normalizedPreferred === 'likes' || normalizedPreferred === 'comments')
    && metricValueFromPayload(metrics, normalizedPreferred) != null
  ) {
    return normalizedPreferred;
  }

  const ordered = metricPreferenceOrder(mediaType);
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
  mediaType: string | null | undefined,
): number {
  if (!isReelLikeMediaType(mediaType)) {
    if (a.key === 'views' && b.key !== 'views') return 1;
    if (b.key === 'views' && a.key !== 'views') return -1;
  }

  const aMultiple = a.multiple ?? Number.NEGATIVE_INFINITY;
  const bMultiple = b.multiple ?? Number.NEGATIVE_INFINITY;
  if (aMultiple !== bMultiple) return bMultiple - aMultiple;

  const aValue = a.value ?? Number.NEGATIVE_INFINITY;
  const bValue = b.value ?? Number.NEGATIVE_INFINITY;
  if (aValue !== bValue) return bValue - aValue;

  const order = metricPreferenceOrder(mediaType);
  return order.indexOf(a.key) - order.indexOf(b.key);
}

export function orderedSupportMetricsFromPayload(
  metrics: MetricPayloadMap,
  bestMetric: FireMetricKey,
  mediaType: string | null | undefined,
): MetricPayloadValue[] {
  return (['views', 'likes', 'comments'] as FireMetricKey[])
    .filter((metric) => metric !== bestMetric)
    .map((metric) => ({
      key: metric,
      value: metricValueFromPayload(metrics, metric),
      multiple: metricMultipleFromPayload(metrics, metric),
      baseline: metricBaselineFromPayload(metrics, metric),
    }))
    .sort((a, b) => compareMetricPayloadValues(a, b, mediaType));
}
