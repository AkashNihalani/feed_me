export type RunSignalKind = 'trend' | 'watch' | 'easy_win' | 'what_changed' | 'durability';

export type RunSignalMetric = {
  label: string;
  value: string;
  detail?: string;
  accent?: boolean;
};

export type RunSignalEvidence = {
  post_key: string;
  post_url: string | null;
  thumbnail_url: string | null;
  title: string;
  placed: string | null;
  views_vs_usual: number | null;
  comments_vs_usual: number | null;
  legs: boolean;
  carried_by: string | null;
  hour_ist: number | null;
};

export type RunSignal = {
  id: string;
  account: string;
  accountLabel: string;
  kind: RunSignalKind;
  headline: string;
  explainer: string;
  generatedAt: string | null;
  runLabel: string;
  metrics: RunSignalMetric[];
  evidence: RunSignalEvidence[];
};
