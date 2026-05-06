export type Timeframe = '7D' | '30D' | '60D' | '90D';

export const TIMEFRAME_TO_DAYS: Record<Timeframe, number> = {
  '7D': 7,
  '30D': 30,
  '60D': 60,
  '90D': 90,
};

export type AscentPoint = {
  snapshot_date_ist: string;
  follower_count: number;
};

export type FrequencyPoint = {
  week_start_ist: string;
  post_count: number;
  avg_percentile_performance: number | null;
  avg_views_percentile: number | null;
  avg_likes_percentile: number | null;
  avg_comments_percentile: number | null;
};

export type HeatmapPoint = {
  day_ist: string;
  post_count: number;
};

export type KillzonePoint = {
  hour_ist: number;
  post_count: number;
};

export type KillzoneDayPoint = {
  day_of_week: number;
  day_label: string;
  post_count: number;
  metric_count: number;
  avg_percentile_performance: number | null;
};

export type ApexMixPoint = {
  media_type: string;
  post_count: number;
  share: number;
};

export type ScatterPoint = {
  post_key: string;
  days_ago: number;
  percentile_performance: number | null;
  views: number | null;
  media_type?: string | null;
  handle: string;
  posted_at_ist: string | null;
};

export type EngagementAverageMediaType = 'all' | 'reel' | 'image' | 'carousel';

export type EngagementAverageRow = {
  media_type: EngagementAverageMediaType;
  post_count: number;
  metric_count: number;
  avg_likes: number | null;
  avg_comments: number | null;
  avg_views: number | null;
};

export type DashboardSummary = {
  window_days: number;
  window_start_ist: string;
  window_end_ist: string;
  post_count: number;
  posts_with_metrics: number;
  avg_percentile_performance: number | null;
  avg_views_percentile: number | null;
  avg_likes_percentile: number | null;
  avg_comments_percentile: number | null;
};

export type PatternBoardSupportPost = {
  post_key: string;
  handle: string | null;
  post_url: string | null;
  thumbnail_url: string | null;
  media_type: string | null;
  post_role: string | null;
  evidence_group: 'main' | 'comparison' | 'unknown';
  evidence_label: string | null;
  evidence_tone: 'positive' | 'negative' | 'reference' | null;
};

export type PatternBoardSignalCard = {
  title: string | null;
  metric_line: string | null;
  read: string | null;
  evidence_pressure: string[];
  confidence: string | null;
};

export type PatternBoardItem = {
  firewatch_id: string;
  signal_code: string;
  context: 'own' | 'cross' | 'anchor';
  pattern_name: string | null;
  pattern_label: string;
  trigger_count: number;
  avg_hot_percentile: number | null;
  feeders_count: number | null;
  baseline_share: number | null;
  recent_lift: number | null;
  anchor_gap: number | null;
  match_count: number | null;
  latest_business_day: string | null;
  cues: string[];
  support_posts: PatternBoardSupportPost[];
  signal_card: PatternBoardSignalCard | null;
};

export type PostingPatternStatus = 'accelerating' | 'steady' | 'slowing' | 'dormant' | 'insufficient_data';

export type PostingPatternMediaShift = {
  current_dominant_type: string | null;
  baseline_dominant_type: string | null;
  share_delta: number | null;
};

export type PostingPatternRhythmDay = {
  day_ist: string;
  post_count: number;
};

export type PostingPatternFeederRow = {
  feeder_id: number;
  handle: string;
  posts_per_week_current: number;
  delta_percent: number | null;
  days_since_last_post: number | null;
  median_gap_hours: number | null;
  dominant_media_type: string | null;
  status: PostingPatternStatus;
};

export type PostingPatternMediaMix = {
  type: string;
  count: number;
  pct: number;
};

export type PostingPatternPayload = {
  status: PostingPatternStatus;
  posts_per_week_current: number;
  posts_per_week_baseline: number;
  delta_percent: number | null;
  last_post_at_ist: string | null;
  days_since_last_post: number | null;
  median_gap_hours: number | null;
  usual_gap_hours: number | null;
  gap_vs_usual_percent: number | null;
  consistency_score: number;
  media_shift: PostingPatternMediaShift;
  media_mix: PostingPatternMediaMix[];
  rhythm_days: PostingPatternRhythmDay[];
  feeder_rows: PostingPatternFeederRow[];
};

export type DashboardPayload = {
  ascent_series: AscentPoint[];
  frequency_series: FrequencyPoint[];
  heatmap_daily: HeatmapPoint[];
  killzone_hours: KillzonePoint[];
  killzone_days?: KillzoneDayPoint[];
  apex_mix: ApexMixPoint[];
  scatter_points: ScatterPoint[];
  engagement_averages?: EngagementAverageRow[];
  summary: DashboardSummary;
  pattern_board?: PatternBoardItem[];
  posting_pattern?: PostingPatternPayload;
};
