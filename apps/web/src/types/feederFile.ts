export type MetricCard = {
  label: string;
  value: string;
  detail?: string;
  accent?: boolean;
};

export type ProofBlock = {
  post_key: string;
  post_url?: string | null;
  proof_label: string;
  proof_headline: string;
  post_read: string;
  what_clicked: string;
  evidence: string[];
  metrics: MetricCard[];
};

export type PatternBreakdown = {
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

export type FeederFilePattern = {
  account: string;
  accountLabel: string;
  accountMeta: string;
  pattern_id: string;
  pattern: PatternBreakdown;
  proofs: ProofBlock[];
  patternMetrics: MetricCard[];
};
