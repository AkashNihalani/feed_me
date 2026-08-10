import type { FollowerRunWindow } from './Followers';

export const FOLLOWER_RUN_WINDOWS = [
  {
    id: 'current',
    label: 'This run',
    range: 'JUL 08 — JUL 14',
    startFollowers: 950_000,
    endFollowers: 958_747,
    baselineDelta: 1_560,
  },
  {
    id: 'previous',
    label: 'Previous run',
    range: 'JUL 01 — JUL 07',
    startFollowers: 947_870,
    endFollowers: 950_000,
    baselineDelta: 1_540,
  },
  {
    id: 'two-runs-ago',
    label: 'Two runs ago',
    range: 'JUN 24 — JUN 30',
    startFollowers: 948_490,
    endFollowers: 947_870,
    baselineDelta: 1_500,
  },
] satisfies FollowerRunWindow[];
