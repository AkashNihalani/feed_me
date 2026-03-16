export type AlertUrgency = 'watch' | 'today' | 'now';

export type FireLayerKey =
  | 'layer_1_position'
  | 'layer_2_distribution'
  | 'layer_3_response'
  | 'layer_4_trajectory'
  | 'layer_5_structural'
  | 'layer_6_timing';

export type FireLayers = Partial<Record<FireLayerKey, Record<string, unknown>>>;

export type FireStamp = {
  handle: string;
  mediaType: string;
  checkpoint: string;
  metricLabel: string;
  metricValue: number | null;
};

export type FirePayload = {
  best_metric?: string;
  metrics?: Record<string, unknown>;
  position?: Record<string, unknown>;
  timing?: Record<string, unknown> | null;
  trajectory?: Record<string, unknown> | null;
  meta?: Record<string, unknown>;
  [key: string]: unknown;
};

export type FireAlertItem = {
  id: string;
  postKey?: string;
  family: 'tracking' | 'insight';
  urgency: AlertUrgency;
  color: string;
  handle: string;
  title: string;
  whyNow: string;
  action: string;
  percentileTag?: string;
  mediaType: string;
  stage: string;
  percentile?: string;
  delta?: string;
  evidence: string[];
  timeAgo: string;
  createdAt: string;
  postUrl?: string;
  thumbnailUrl?: string;
  businessDateKey: string;
  businessDateIst?: string;
  status?: string;

  surfacePercentile: number | null;
  surfaceDelta: number | null;
  trajectoryDeltaPercentile: number | null;
  surfaceHandle: string;
  surfaceMediaType: string;
  checkpoint: string;
  metricValue: number | null;
  metricKey: string;

  stamp?: FireStamp;
  payload: FirePayload;
  layers: FireLayers;
};

// Backward compatibility while migrating references.
export type FireItem = FireAlertItem;
