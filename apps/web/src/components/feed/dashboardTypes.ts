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
  handle: string;
  posted_at_ist: string | null;
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

export type DashboardPayload = {
  ascent_series: AscentPoint[];
  frequency_series: FrequencyPoint[];
  heatmap_daily: HeatmapPoint[];
  killzone_hours: KillzonePoint[];
  apex_mix: ApexMixPoint[];
  scatter_points: ScatterPoint[];
  summary: DashboardSummary;
};
