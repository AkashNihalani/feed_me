export type AlertFamily = 'velocity' | 'competitive' | 'intelligence';
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
  velocityTag: string; // 🔥, 🚀, 👁
  stage: string;       // D3, D7
  percentile?: string; // 08%
  evidence: string[];
  timeAgo: string;
  createdAt: string;
  postUrl: string;
  thumbnailUrl?: string;
  businessDateKey: string;
};
