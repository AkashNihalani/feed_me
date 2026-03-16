export type Timeframe = '4W' | '12W' | '26W' | '52W';

export const TIMEFRAME_TO_WEEKS: Record<Timeframe, number> = {
  '4W': 4,
  '12W': 12,
  '26W': 26,
  '52W': 52,
};

export type AscentPoint = {
  week_start_ist: string;
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

export type DashboardPayload = {
  ascent_series: AscentPoint[];
  frequency_series: FrequencyPoint[];
  heatmap_daily: HeatmapPoint[];
  killzone_hours: KillzonePoint[];
  apex_mix: ApexMixPoint[];
  scatter_points: ScatterPoint[];
};

