export type AlertFamily = 'insight' | 'stack';
export type AlertUrgency = 'now' | 'today' | 'watch';

export type FireItem = {
  id: string;
  family: AlertFamily;
  urgency: AlertUrgency;
  color: string;
  handle: string;
  title: string;
  whyNow: string;
  action: string;
  percentileTag: string;
  mediaType?: string;
  stage: string;
  percentile?: string;
  delta?: string;
  evidence: string[];
  timeAgo: string;
  createdAt: string;
  postUrl: string;
  thumbnailUrl?: string; // Standard high-res image
  businessDateKey: string;
  
  // Layout Props (Computed on frontend)
  colSpan?: 1 | 2 | 3; // Grid span
  stackCount?: number; // If family === 'stack'
};
