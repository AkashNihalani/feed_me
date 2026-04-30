export type AlertUrgency = 'watch' | 'today' | 'now';
export type FireCardKind = 'tracking';

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

export type FireFeederOption = {
  id: number;
  handle: string;
};

export type FireFeedOption = {
  id: number;
  name: string;
  feeders: FireFeederOption[];
};

export type FireFilterThreshold = '10' | '25' | '50' | 'ALL';
export type FireMediaFilter = 'IMAGE' | 'CAROUSEL' | 'REEL' | 'ALL';
export type FireSortMode = 'best' | 'recent';

export type FireFilterState = {
  threshold: FireFilterThreshold;
  mediaFilter: FireMediaFilter;
  sort: FireSortMode;
  selectedFeedIds: number[];
  selectedFeederIdsByFeed: Record<string, number[]>;
  selectedCheckpoints: string[];
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

export type WarmupMediaBucket = 'REEL' | 'CAROUSEL' | 'IMAGE';

export type FireWarmupGate = {
  bucket: WarmupMediaBucket;
  count: number;
  required: number;
  isLocked: boolean;
  headline: string;
  body: string;
  progressLabel: string;
};

export type FireAlertItem = {
  id: string;
  cardKind: FireCardKind;
  postKey?: string;
  feedId?: number;
  feederId?: number;
  urgency: AlertUrgency;
  color: string;
  signalCode: string;
  signalContext: 'own' | 'cross' | 'anchor';
  signalLabel: string;
  signalHeadline: string;
  handle: string;
  title: string;
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
  previewUrl?: string;
  businessDateKey: string;
  businessDateIst?: string;
  status?: string;

  surfacePercentile: number | null;
  surfacePercentileExact?: number | null;
  surfaceDelta: number | null;
  trajectoryDeltaPercentile: number | null;
  surfaceHandle: string;
  surfaceMediaType: string;
  checkpoint: string;
  postedAt?: string;
  metricValue: number | null;
  metricKey: string;

  stamp?: FireStamp;
  payload: FirePayload;
  layers: FireLayers;
  intelligenceSkipped?: boolean;
  hideSignalChrome?: boolean;
  warmupGate?: FireWarmupGate | null;
};

// Backward compatibility while migrating references.
export type FireItem = FireAlertItem;
