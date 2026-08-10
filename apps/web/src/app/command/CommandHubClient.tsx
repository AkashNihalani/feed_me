'use client';

/* eslint-disable @next/next/no-img-element -- command thumbnails use dynamic read-only media URLs. */

import type { CSSProperties } from 'react';
import { useCallback, useDeferredValue, useEffect, useLayoutEffect, useMemo, useRef, useState, useSyncExternalStore } from 'react';
import { AnimatePresence, motion, useReducedMotion, type MotionProps } from 'framer-motion';
import {
  AlertTriangle,
  Bell,
  Boxes,
  Brain,
  Check,
  ChevronRight,
  ChevronsRight,
  CircleDollarSign,
  Clock3,
  Compass,
  Copy,
  ExternalLink,
  Flame,
  Gauge,
  HardDrive,
  History,
  Image as ImageIcon,
  Layers3,
  Network,
  Play,
  RefreshCw,
  Search,
  ShieldCheck,
  TimerReset,
  Video,
} from 'lucide-react';
import RollingNumber from '@/components/RollingNumber';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import {
  commandEventTime,
  partitionCommandRunway,
  type CommandEvent,
} from './commandViewModel';

type Dict = Record<string, unknown>;
type DomainId =
  | 'overview'
  | 'feeds'
  | 'activity'
  | 'pending'
  | 'failures'
  | 'account'
  | 'engine'
  | 'checkpoints'
  | 'fire'
  | 'intelligence'
  | 'media'
  | 'notifications'
  | 'finance'
  | 'gaps';
type StatusKind = 'live' | 'stale' | 'failed' | 'missing' | 'partial' | 'readonly' | 'info';

type CommandRow = {
  id: string;
  title: string;
  eyebrow: string;
  subtitle: string;
  status: string;
  metric: string;
  updatedAt: string | null;
  source: string;
  raw: Dict;
};

type CommandPayload = {
  generatedAt?: string;
  access?: Dict;
  topline?: Dict;
  accountGraph?: Dict;
  feedOps?: Dict;
  engine?: Dict;
  checkpoints?: Dict;
  fireSignals?: Dict;
  intelligence?: Dict;
  media?: Dict;
  notifications?: Dict;
  finance?: Dict;
  runtime?: Dict;
  timeline?: Dict;
  operationalReadiness?: Dict;
  sources?: Dict;
  productSurfaces?: Dict[];
  instrumentationGaps?: Dict[];
};

type SystemHealth = {
  id: DomainId;
  label: string;
  status: StatusKind;
  value: string;
  note: string;
  updatedAt: string | null;
};

type Attention = {
  id: string;
  label: string;
  source: string;
  detail: string;
  status: StatusKind;
  updatedAt: string | null;
  row?: CommandRow;
  domain?: DomainId;
};

type LoadFailure = {
  status: number;
  message: string;
};

type PipelineMetric = {
  label: string;
  value: string;
  tone?: StatusKind;
};

type OpsListTime = 'recent' | 'upcoming';
type NavigationMotion = 'fluid' | 'instant';
type NavigationDirection = 'forward' | 'backward' | 'neutral';
type CommandMotionPart = 'summary' | 'controls' | 'body' | 'inspector';

type CommandMotionState = {
  direction: NavigationDirection;
  mode: NavigationMotion;
  compact: boolean;
  reduced: boolean;
};
type CommandRegionMotionProps = Pick<MotionProps, 'initial' | 'animate' | 'exit' | 'transition'>;

type RuntimeSurface = {
  id: 'workers' | 'schedules';
  label: string;
  status: StatusKind;
  value: string;
  note: string;
  updatedAt: string | null;
};

type RunwayTone = 'overdue' | 'running' | 'due' | 'later';

type RunwayGroup = {
  id: string;
  feedName: string;
  feederHandle: string;
  events: Dict[];
  first: Dict;
};

const navItems: { id: DomainId; label: string; icon: typeof Gauge; group: string }[] = [
  { id: 'overview', label: 'Overview', icon: Gauge, group: 'command' },
  { id: 'feeds', label: 'Feeds', icon: Network, group: 'command' },
  { id: 'activity', label: 'Activity', icon: TimerReset, group: 'command' },
  { id: 'pending', label: 'Pending', icon: Layers3, group: 'command' },
  { id: 'failures', label: 'Failures', icon: Flame, group: 'command' },
  { id: 'gaps', label: 'Gaps', icon: AlertTriangle, group: 'command' },
];

const domainNavItems: { id: DomainId; label: string; icon: typeof Gauge; group: string }[] = [
  { id: 'account', label: 'Account Graph', icon: Network, group: 'domain' },
  { id: 'engine', label: 'Engine', icon: TimerReset, group: 'domain' },
  { id: 'checkpoints', label: 'Checkpoints', icon: Layers3, group: 'domain' },
  { id: 'fire', label: 'Fire / Signals', icon: Flame, group: 'domain' },
  { id: 'intelligence', label: 'Intelligence', icon: Brain, group: 'domain' },
  { id: 'media', label: 'Media', icon: HardDrive, group: 'domain' },
  { id: 'notifications', label: 'Notifications', icon: Bell, group: 'domain' },
  { id: 'finance', label: 'Finance', icon: CircleDollarSign, group: 'domain' },
];
const allNavItems = [...navItems, ...domainNavItems];
const domainIds = new Set<DomainId>(allNavItems.map((item) => item.id));
const neutralDomains = new Set<DomainId>(['intelligence', 'finance']);
const primaryTaskNav: { id: DomainId; label: string; icon: typeof Gauge }[] = [
  { id: 'overview', label: 'Now', icon: Gauge },
  { id: 'pending', label: 'Next', icon: ChevronsRight },
  { id: 'activity', label: 'History', icon: History },
  { id: 'feeds', label: 'Explore', icon: Compass },
];
const primaryTaskIds = new Set<DomainId>(['overview', 'pending', 'activity']);
const exploreNavItems = allNavItems.filter((item) => !primaryTaskIds.has(item.id));
const navigationPosition = (domain: DomainId) => {
  if (domain === 'overview') return 0;
  if (domain === 'pending') return 1;
  if (domain === 'activity') return 2;
  return 3;
};

const commandMotionQuery = '(max-width: 880px)';
const commandEaseOut = [0.23, 1, 0.32, 1] as const;
const commandRestingTransform = 'translate3d(0px, 0px, 0px) scale(1)';
const commandRegionOrder: Record<CommandMotionPart, number> = {
  summary: 0,
  controls: 1,
  body: 2,
  inspector: 3,
};
const commandRegionOpacity: Record<CommandMotionPart, number> = {
  summary: 0.99,
  controls: 0.985,
  body: 0.975,
  inspector: 0.98,
};
const commandEnterSpring = {
  type: 'spring',
  stiffness: 420,
  damping: 38,
  mass: 0.72,
} as const;
const commandIndicatorSpring = {
  type: 'spring',
  stiffness: 440,
  damping: 44,
  mass: 0.72,
} as const;
const commandLayoutSpring = {
  type: 'spring',
  stiffness: 360,
  damping: 32,
  mass: 0.8,
} as const;

function subscribeCommandMotionViewport(onChange: () => void) {
  const query = window.matchMedia(commandMotionQuery);
  query.addEventListener('change', onChange);
  return () => query.removeEventListener('change', onChange);
}

function commandMotionViewportSnapshot() {
  return window.matchMedia(commandMotionQuery).matches;
}

function commandMotionViewportServerSnapshot() {
  return false;
}

function commandInitialTransform(part: CommandMotionPart, state: CommandMotionState) {
  const distance = part === 'summary'
    ? state.compact ? 3 : 4
    : part === 'controls'
      ? state.compact ? 4 : 5
      : part === 'body'
        ? state.compact ? 6 : 8
        : state.compact ? 5 : 6;
  const scale = part === 'body' ? 0.994 : part === 'summary' ? 0.997 : 0.996;
  return `translate3d(0px, ${distance}px, 0px) scale(${scale})`;
}

function commandExitTransform(part: CommandMotionPart, state: CommandMotionState) {
  const distance = part === 'body'
    ? state.compact ? 3 : 4
    : part === 'inspector'
      ? state.compact ? 2 : 3
      : 2;
  return `translate3d(0px, -${distance}px, 0px) scale(${part === 'body' ? 0.997 : 0.998})`;
}

function commandRegionMotion(part: CommandMotionPart, state: CommandMotionState): CommandRegionMotionProps {
  const stableExploreChrome = state.direction === 'neutral' && (part === 'summary' || part === 'controls');
  if (state.mode === 'instant' || stableExploreChrome) {
    return {
      initial: false as const,
      animate: { opacity: 1, transform: commandRestingTransform },
      exit: { opacity: 1, transform: commandRestingTransform },
      transition: { duration: 0 },
    };
  }
  if (state.reduced) {
    return {
      initial: { opacity: 0.94, transform: commandRestingTransform },
      animate: { opacity: 1, transform: commandRestingTransform },
      exit: { opacity: 0.06, transform: commandRestingTransform },
      transition: {
        opacity: { duration: 0.1, ease: commandEaseOut },
        transform: { duration: 0 },
      },
    };
  }

  const stagger = state.compact ? 0.035 : 0.04;
  const delay = state.direction === 'neutral'
    ? part === 'inspector' ? stagger : 0
    : commandRegionOrder[part] * stagger;

  return {
    initial: {
      opacity: commandRegionOpacity[part],
      transform: commandInitialTransform(part, state),
    },
    animate: { opacity: 1, transform: commandRestingTransform },
    exit: {
      opacity: 0,
      transform: commandExitTransform(part, state),
      transition: {
        opacity: { duration: 0.14, ease: commandEaseOut },
        transform: { duration: 0.18, ease: commandEaseOut },
      },
    },
    transition: {
      transform: { ...commandEnterSpring, delay },
      opacity: { duration: 0.16, ease: commandEaseOut, delay },
    },
  };
}

const statusOrder: Record<StatusKind, number> = {
  failed: 0,
  missing: 1,
  stale: 2,
  partial: 3,
  info: 4,
  readonly: 5,
  live: 6,
};

const pipelineLabels: Record<DomainId, string[]> = {
  overview: ['Risk', 'Freshness', 'Records', 'Inspector'],
  feeds: ['Smooth', 'Pending', 'Failed', 'Missing'],
  activity: ['Recent', 'Feeds', 'Feeders', 'Records'],
  pending: ['Queued', 'Next', 'Feeders', 'Failures'],
  failures: ['Failed', 'Feeds', 'Feeders', 'Latest'],
  account: ['Users', 'Feeds', 'Feeders', 'Context'],
  engine: ['Queued', 'Running', 'Retry', 'Failed'],
  checkpoints: ['D1', 'D3', 'D7', 'D21'],
  fire: ['Tracking', 'Signals', 'Hot Posts', 'Suppressed'],
  intelligence: ['Connection', 'Fingerprints', 'Condense', 'Model Calls'],
  media: ['Active', 'Capture', 'Purge', 'Failed'],
  notifications: ['Subscriptions', 'Pending', 'Sent', 'Failed'],
  finance: ['Connection', 'Ledger', 'Revenue', 'Costs'],
  gaps: ['Cost', 'Health', 'Traffic', 'Finance'],
};

function dict(value: unknown): Dict {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Dict : {};
}

function list(value: unknown): Dict[] {
  return Array.isArray(value) ? value.filter((item): item is Dict => Boolean(item && typeof item === 'object' && !Array.isArray(item))) : [];
}

function str(value: unknown, fallback = ''): string {
  if (typeof value === 'string' && value.trim()) return value;
  if (typeof value === 'number' && Number.isFinite(value)) return String(value);
  return fallback;
}

function num(value: unknown): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function formatNumber(value: unknown) {
  return new Intl.NumberFormat('en-IN', { maximumFractionDigits: 1 }).format(num(value));
}

function formatInr(value: unknown) {
  return new Intl.NumberFormat('en-IN', {
    style: 'currency',
    currency: 'INR',
    maximumFractionDigits: 0,
  }).format(num(value));
}

function formatBytes(value: unknown) {
  const bytes = num(value);
  if (!bytes) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const amount = bytes / (1024 ** exponent);
  return `${amount >= 10 ? Math.round(amount) : amount.toFixed(1)} ${units[exponent]}`;
}

function labelize(value: unknown, fallback = 'Unknown') {
  const raw = str(value, fallback);
  return raw
    .replace(/[_-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function shortDate(value: unknown, withSeconds = false) {
  const raw = str(value);
  if (!raw) return 'No timestamp';
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return 'No timestamp';
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    second: withSeconds ? '2-digit' : undefined,
  }).format(date);
}

function relativeTime(value: unknown) {
  const raw = str(value);
  if (!raw) return 'missing';
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return 'missing';
  const deltaMinutes = Math.round((date.getTime() - Date.now()) / 60000);
  const future = deltaMinutes > 0;
  const minutes = Math.abs(deltaMinutes);
  if (minutes < 1) return 'now';
  const amount = minutes < 60
    ? `${minutes}m`
    : Math.round(minutes / 60) < 48
      ? `${Math.round(minutes / 60)}h`
      : `${Math.round(minutes / 1440)}d`;
  return future ? `in ${amount}` : `${amount} ago`;
}

function isDomainId(value: string | null): value is DomainId {
  return Boolean(value && domainIds.has(value as DomainId));
}

function queryMatches(value: unknown, needle: string): boolean {
  if (!needle) return true;
  try {
    return (JSON.stringify(value) ?? String(value)).toLowerCase().includes(needle);
  } catch {
    return String(value).toLowerCase().includes(needle);
  }
}

function statusCount(value: unknown, ...keys: string[]): number {
  const counts = dict(value);
  return keys.reduce((total, key) => {
    const exact = Object.entries(counts).find(([candidate]) => candidate.toLowerCase() === key.toLowerCase());
    return total + num(exact?.[1]);
  }, 0);
}

function metricNumber(source: Dict, key: string, fallback = 0): number {
  return Object.prototype.hasOwnProperty.call(source, key) ? num(source[key]) : fallback;
}

function sourceAwareStatus(section: Dict, current: StatusKind): StatusKind {
  const sourceStatus = str(dict(section.sourceHealth).status).toLowerCase();
  if (sourceStatus === 'unavailable') return 'failed';
  if (sourceStatus === 'degraded' && current !== 'failed') return 'partial';
  return current;
}

function statusKind(status: unknown): StatusKind {
  const key = str(status).toLowerCase();
  if (['failed', 'error', 'unavailable', 'capture_failed', 'purge_failed'].includes(key)) return 'failed';
  if (key.includes('missing')) return 'missing';
  if (key === 'stale') return 'stale';
  if (['partial', 'configuration_needed', 'pending', 'retry', 'running', 'capturing'].includes(key)) return 'partial';
  if (['active', 'fresh', 'done', 'sent', 'paid', 'enabled'].includes(key)) return 'live';
  return 'info';
}

function latestAt(rows: CommandRow[]) {
  return rows
    .map((row) => str(row.updatedAt))
    .filter(Boolean)
    .sort((a, b) => Date.parse(b) - Date.parse(a))[0] || null;
}

function makeRow(raw: Dict, fallback: Partial<CommandRow>): CommandRow {
  const id = str(raw.id) || str(raw.post_key) || str(raw.call_key) || fallback.id || crypto.randomUUID();
  return {
    id,
    title: fallback.title || str(raw.handle) || str(raw.name) || str(raw.feeder_handle) || str(raw.post_key) || `Row ${id}`,
    eyebrow: fallback.eyebrow || labelize(raw.job_type || raw.call_type || raw.asset_role || raw.signal_family || raw.kind || 'record'),
    subtitle: fallback.subtitle || str(raw.last_error) || str(raw.body) || str(raw.email) || str(raw.post_key) || 'Read-only record',
    status: fallback.status || str(raw.status, 'active'),
    metric: fallback.metric || '',
    updatedAt: fallback.updatedAt || str(raw.updated_at) || str(raw.created_at) || str(raw.generated_at) || str(raw.completed_at) || null,
    source: fallback.source || 'FeedMe',
    raw,
  };
}

function rowsForDomain(payload: CommandPayload | null, domain: DomainId): CommandRow[] {
  if (!payload) return [];
  if (['feeds', 'activity', 'pending', 'failures'].includes(domain)) return [];
  const account = dict(payload.accountGraph);
  const engine = dict(payload.engine);
  const checkpoints = dict(payload.checkpoints);
  const fire = dict(payload.fireSignals);
  const intelligence = dict(payload.intelligence);
  const media = dict(payload.media);
  const notifications = dict(payload.notifications);
  const finance = dict(payload.finance);
  const artifacts = dict(intelligence.artifacts);

  if (domain === 'account') {
    return [
      ...list(account.feeders).map((row) => makeRow(row, {
        title: str(row.handle, 'Unknown feeder'),
        eyebrow: `${labelize(row.role, 'Feeder')} feeder`,
        subtitle: `Feed #${str(row.feed_id)} / ${formatNumber(row.follower_count)} followers`,
        metric: labelize(row.context_role, 'Context'),
        source: 'feeders',
      })),
      ...list(account.feeds).map((row) => makeRow(row, {
        title: str(row.name, 'Untitled feed'),
        eyebrow: 'Feed',
        subtitle: `Owner ${str(row.user_id, 'unknown user')}`,
        metric: 'context',
        source: 'feeds',
      })),
    ];
  }

  if (domain === 'engine') {
    return list(engine.recentJobs).map((row) => makeRow(row, {
      title: `${labelize(row.job_type, 'Run')} / feeder #${str(row.feeder_id)}`,
      eyebrow: 'Run job',
      subtitle: str(row.last_error, `Business day ${str(row.business_date_ist, 'not set')}`),
      metric: `attempt ${formatNumber(row.attempt)}`,
      source: 'run_jobs',
    }));
  }

  if (domain === 'checkpoints') {
    return [
      ...list(checkpoints.recentJobs).map((row) => makeRow(row, {
        title: str(row.feeder_handle) ? `@${str(row.feeder_handle)}` : str(row.post_key, 'Unknown post'),
        eyebrow: 'Checkpoint job',
        subtitle: str(row.post_key, str(row.last_error, `Next run ${shortDate(row.next_run_at)}`)),
        metric: `attempt ${formatNumber(row.attempt)}`,
        source: 'checkpoint_jobs',
      })),
      ...list(checkpoints.recentMetrics).map((row) => makeRow(row, {
        title: str(row.feeder_handle) ? `@${str(row.feeder_handle)}` : str(row.post_key, 'Unknown post'),
        eyebrow: 'Metric surface',
        subtitle: str(row.post_key, 'Metric row'),
        status: 'done',
        metric: `${formatNumber(row.views)} views`,
        updatedAt: str(row.computed_at),
        source: 'post_metrics',
      })),
    ];
  }

  if (domain === 'fire') {
    return [
      ...list(fire.recentAlerts).map((row) => makeRow(row, {
        title: `${labelize(row.alert_type, 'Alert')} / ${str(row.post_key, 'post')}`,
        eyebrow: labelize(row.signal_code, 'Fire alert'),
        subtitle: str(row.body, `${labelize(row.checkpoint, 'checkpoint')} / ${str(row.metric_key, 'metric')}`),
        metric: str(row.surface_percentile, 'surface'),
        source: 'fire_alerts',
      })),
      ...list(fire.recentSignals).map((row) => makeRow(row, {
        title: labelize(row.signal_type, 'Signal'),
        eyebrow: labelize(row.signal_family, 'Signal family'),
        subtitle: str(row.body, `${labelize(row.scope, 'scope')} / ${labelize(row.checkpoint, 'checkpoint')}`),
        metric: str(row.business_date_ist),
        updatedAt: str(row.last_fired_at) || str(row.updated_at),
        source: 'signals',
      })),
    ];
  }

  if (domain === 'intelligence') {
    return [
      ...list(intelligence.recentModelCalls).map((row) => makeRow(row, {
        title: `${labelize(row.call_type, 'Model call')} / ${str(row.feeder_handle, 'unknown')}`,
        eyebrow: str(row.model, 'model'),
        subtitle: str(row.error, `${str(row.prompt_version, 'prompt')} / ${str(row.post_key, 'no post')}`),
        metric: str(row.pattern_id, str(row.call_key, 'audit')),
        updatedAt: str(row.completed_at) || str(row.updated_at),
        source: 'feeder_file_model_calls',
      })),
      ...list(artifacts.feederFiles).map((row) => makeRow(row, {
        title: str(row.feeder_handle, 'Feeder file'),
        eyebrow: 'Feeder file',
        subtitle: `${str(row.compile_version, 'compile')} / ${str(row.active_window, 'window unknown')}`,
        source: 'feeder_files',
      })),
      ...list(artifacts.condensations).map((row) => makeRow(row, {
        title: str(row.post_key, 'Post condensation'),
        eyebrow: 'Condensation',
        subtitle: str(row.condensation_version, 'version unknown'),
        status: 'done',
        source: 'post_condensations',
      })),
    ];
  }

  if (domain === 'media') {
    return list(media.recentAssets).map((row) => makeRow(row, {
      title: `${labelize(row.asset_role, 'Asset')} / ${str(row.post_key, 'post')}`,
      eyebrow: str(row.storage_bucket, 'storage'),
      subtitle: str(row.last_error, `${str(row.mime_type, 'mime unknown')} / ${formatBytes(row.byte_size)}`),
      metric: formatBytes(row.byte_size),
      source: 'post_media_assets',
    }));
  }

  if (domain === 'notifications') {
    return [
      ...list(notifications.recentJobs).map((row) => makeRow(row, {
        title: `${labelize(row.kind, 'Push')} job / ${str(row.dedupe_key, 'dedupe')}`,
        eyebrow: 'Push job',
        subtitle: str(row.last_error, `Feed ${str(row.feed_id, 'unknown')}`),
        metric: `attempt ${formatNumber(row.attempt)}`,
        source: 'web_push_jobs',
      })),
      ...list(notifications.recentSubscriptions).map((row) => makeRow(row, {
        title: `Subscription #${str(row.id)}`,
        eyebrow: 'Push subscription',
        subtitle: str(row.last_error, `Last seen ${shortDate(row.last_seen_at)}`),
        status: row.enabled === true ? 'active' : 'paused',
        metric: row.enabled === true ? 'enabled' : 'disabled',
        source: 'web_push_subscriptions',
      })),
    ];
  }

  if (domain === 'finance') {
    return list(finance.recentTransactions).map((row) => makeRow(row, {
      title: `Transaction ${str(row.id, 'unknown')}`,
      eyebrow: 'Razorpay reference',
      subtitle: `User ${str(row.user_id, 'unknown')}`,
      metric: formatInr(num(row.amount) / 100),
      source: 'transactions',
    }));
  }

  if (domain === 'gaps') {
    return list(payload.instrumentationGaps).map((row) => makeRow(row, {
      id: str(row.id, 'gap'),
      title: str(row.label, 'Instrumentation gap'),
      eyebrow: 'Missing instrumentation',
      subtitle: str(row.detail, 'No detail available'),
      status: str(row.status, 'missing_instrumentation'),
      metric: str(row.path, 'instrument'),
      source: 'instrumentation',
    }));
  }

  return [
    ...rowsForDomain(payload, 'engine').slice(0, 5),
    ...rowsForDomain(payload, 'checkpoints').slice(0, 5),
    ...rowsForDomain(payload, 'fire').slice(0, 5),
    ...rowsForDomain(payload, 'media').slice(0, 4),
    ...rowsForDomain(payload, 'gaps').slice(0, 6),
  ];
}

function systemHealth(payload: CommandPayload | null): SystemHealth[] {
  if (!payload) return [];
  const topline = dict(payload.topline);
  const account = dict(payload.accountGraph);
  const engine = dict(payload.engine);
  const checkpoints = dict(payload.checkpoints);
  const fire = dict(payload.fireSignals);
  const intelligence = dict(payload.intelligence);
  const media = dict(payload.media);
  const notifications = dict(payload.notifications);
  const engineTotals = dict(engine.totals);
  const checkpointTotals = dict(checkpoints.totals);
  const fireTotals = dict(fire.totals);
  const mediaTotals = dict(media.totals);
  const notificationTotals = dict(notifications.totals);
  const gapCount = list(payload.instrumentationGaps).length;

  return [
    {
      id: 'account',
      label: 'Account',
      status: sourceAwareStatus(account, num(topline.activeFeeders) ? 'live' : 'stale'),
      value: formatNumber(topline.activeFeeders),
      note: `${formatNumber(topline.followerReach)} reach`,
      updatedAt: str(account.latestChangeAt) || latestAt(rowsForDomain(payload, 'account')),
    },
    {
      id: 'engine',
      label: 'Engine',
      status: sourceAwareStatus(engine, num(engineTotals.failed) ? 'failed' : num(engineTotals.open) ? 'partial' : 'live'),
      value: formatNumber(engineTotals.jobs),
      note: `${formatNumber(engineTotals.successPercent)}% success`,
      updatedAt: str(engine.latestChangeAt) || latestAt(rowsForDomain(payload, 'engine')),
    },
    {
      id: 'checkpoints',
      label: 'Checkpoints',
      status: sourceAwareStatus(checkpoints, num(checkpointTotals.failed) ? 'failed' : num(checkpointTotals.open) ? 'partial' : 'live'),
      value: formatNumber(checkpointTotals.jobs),
      note: `${formatNumber(checkpointTotals.metrics)} metric rows`,
      updatedAt: str(checkpoints.latestChangeAt) || latestAt(rowsForDomain(payload, 'checkpoints')),
    },
    {
      id: 'fire',
      label: 'Fire',
      status: sourceAwareStatus(fire, num(fireTotals.erroredSignals) ? 'failed' : num(fireTotals.staleOrSuppressedSignals) ? 'stale' : 'live'),
      value: formatNumber(num(fireTotals.alerts) + num(fireTotals.signals)),
      note: `${formatNumber(fireTotals.hotPosts)} hot posts`,
      updatedAt: str(fire.latestChangeAt) || latestAt(rowsForDomain(payload, 'fire')),
    },
    {
      id: 'intelligence',
      label: 'Intelligence',
      status: 'readonly',
      value: 'NOT CONNECTED',
      note: 'excluded from readiness',
      updatedAt: str(intelligence.latestChangeAt) || latestAt(rowsForDomain(payload, 'intelligence')),
    },
    {
      id: 'media',
      label: 'Media',
      status: sourceAwareStatus(media, num(mediaTotals.failed) ? 'failed' : num(mediaTotals.pendingCapture) || num(mediaTotals.purgeQueue) ? 'partial' : 'live'),
      value: formatBytes(mediaTotals.knownBytes),
      note: `${formatNumber(mediaTotals.assets)} assets`,
      updatedAt: str(media.latestChangeAt) || latestAt(rowsForDomain(payload, 'media')),
    },
    {
      id: 'notifications',
      label: 'Notifications',
      status: sourceAwareStatus(
        notifications,
        num(notificationTotals.failedJobs)
          ? 'failed'
          : notifications.productionDeliveryWired === false || num(notificationTotals.openJobs)
            ? 'partial'
            : 'live',
      ),
      value: formatNumber(notificationTotals.jobs),
      note: notifications.productionDeliveryWired === false
        ? `${formatNumber(notificationTotals.activeSubscriptions)} subs / producer not wired`
        : `${formatNumber(notificationTotals.activeSubscriptions)} active subs`,
      updatedAt: str(notifications.latestChangeAt) || latestAt(rowsForDomain(payload, 'notifications')),
    },
    {
      id: 'finance',
      label: 'Finance',
      status: 'readonly',
      value: 'NOT CONNECTED',
      note: 'excluded from readiness',
      updatedAt: latestAt(rowsForDomain(payload, 'finance')),
    },
    {
      id: 'gaps',
      label: 'Gaps',
      status: gapCount ? 'missing' : 'live',
      value: formatNumber(gapCount),
      note: 'instrumentation gaps',
      updatedAt: payload.generatedAt || null,
    },
  ];
}

function runtimeHealth(payload: CommandPayload | null): RuntimeSurface[] {
  if (!payload?.runtime) return [];
  const runtime = dict(payload.runtime);
  const workers = dict(runtime.workers);
  const workerTotals = dict(workers.totals);
  const schedules = dict(runtime.schedules);
  const scheduleTotals = dict(schedules.totals);
  const sourceStates = dict(payload.sources);
  const workerSource = dict(sourceStates.workerSnapshot);
  const scheduleSource = dict(sourceStates.scheduleSnapshot);
  const runtimeQueried = workerSource.queried === true
    || scheduleSource.queried === true
    || list(workers.rows).length > 0
    || list(schedules.rows).length > 0;
  if (!runtimeQueried) return [];
  const workerCount = num(workerTotals.workers);
  const scheduleCount = num(scheduleTotals.schedules);
  const workerStatus: StatusKind = num(workerTotals.offline)
    ? 'failed'
    : num(workerTotals.stale)
      ? 'stale'
      : workerCount
        ? 'live'
        : 'missing';
  const scheduleStatus: StatusKind = (
    num(scheduleTotals.failed)
    || num(scheduleTotals.missed)
    || num(scheduleTotals.unregistered)
  )
    ? 'failed'
    : num(scheduleTotals.late)
      ? 'stale'
      : num(scheduleTotals.unobserved)
        ? 'partial'
        : scheduleCount
          ? 'live'
          : 'missing';
  return [
    {
      id: 'workers',
      label: 'Workers',
      status: workerStatus,
      value: `${formatNumber(workerTotals.healthy)} / ${formatNumber(workerCount)}`,
      note: `${formatNumber(workerTotals.stale)} stale / ${formatNumber(workerTotals.offline)} offline`,
      updatedAt: str(workers.latestChangeAt) || null,
    },
    {
      id: 'schedules',
      label: 'Schedules',
      status: scheduleStatus,
      value: `${formatNumber(scheduleTotals.healthy)} / ${formatNumber(scheduleCount)}`,
      note: `${formatNumber(scheduleTotals.late)} late / ${formatNumber(scheduleTotals.missed)} missed / ${formatNumber(scheduleTotals.unregistered)} drift / ${formatNumber(scheduleTotals.failed)} failed`,
      updatedAt: str(schedules.latestChangeAt) || str(schedules.nextRunAt) || null,
    },
  ];
}

function normalizeTimelineEvent(raw: Dict): Dict {
  const happenedAt = str(raw.happenedAt)
    || str(raw.happened_at)
    || str(raw.occurred_at)
    || str(raw.created_at)
    || str(raw.updated_at);
  const eventType = str(raw.eventType) || str(raw.event_type) || str(raw.kind, 'Operational event');
  const severity = str(raw.severity).toLowerCase();
  return {
    ...raw,
    id: str(raw.id, `${str(raw.source, 'ops_events')}:${eventType}:${happenedAt}`),
    title: str(raw.title) || labelize(eventType),
    source: str(raw.source, 'ops_events'),
    kind: eventType,
    status: str(raw.status) || (severity === 'critical' ? 'failed' : severity === 'warning' ? 'pending' : 'done'),
    detail: str(raw.detail) || str(raw.summary) || str(raw.message) || str(raw.entity_id, 'Recorded operational event'),
    feedName: str(raw.feedName) || str(raw.feed_name),
    feederHandle: str(raw.feederHandle) || str(raw.feeder_handle),
    postKey: str(raw.postKey) || str(raw.post_key),
    happenedAt,
  };
}

function attentionQueue(payload: CommandPayload | null, systems: SystemHealth[]): Attention[] {
  if (!payload) return [];
  const rows = rowsForDomain(payload, 'overview');
  const domainForSource = (source: string): DomainId | undefined => {
    if (source === 'run_jobs') return 'engine';
    if (['checkpoint_jobs', 'post_metrics'].includes(source)) return 'checkpoints';
    if (['fire_alerts', 'signals'].includes(source)) return 'fire';
    if (source === 'post_media_assets') return 'media';
    if (source === 'web_push_jobs') return 'notifications';
    if (source === 'instrumentation') return 'gaps';
    return undefined;
  };
  const rowAttention = rows
    .filter((row) => statusKind(row.status) !== 'live')
    .map((row) => ({
      id: `${row.source}:${row.id}`,
      label: row.title,
      source: row.source,
      detail: row.subtitle,
      status: statusKind(row.status),
      updatedAt: row.updatedAt,
      row,
      domain: domainForSource(row.source),
    }));
  const systemAttention = systems
    .filter((system) => ['failed', 'missing', 'stale', 'partial'].includes(system.status))
    .map((system) => ({
      id: `system:${system.id}`,
      label: system.label,
      source: 'system',
      detail: system.note,
      status: system.status,
      updatedAt: system.updatedAt,
      domain: system.id,
    }));

  return [...systemAttention, ...rowAttention]
    .sort((a, b) => statusOrder[a.status] - statusOrder[b.status]);
}

function StatusChip({ status, label }: { status: StatusKind | string; label?: string }) {
  const kind = typeof status === 'string' && status in statusOrder ? status as StatusKind : statusKind(status);
  const text = label || (
    kind === 'live' ? 'LIVE'
      : kind === 'stale' ? 'STALE'
        : kind === 'failed' ? 'FAILED'
          : kind === 'missing' ? 'MISSING'
            : kind === 'partial' ? 'PARTIAL'
              : kind === 'readonly' ? 'READ ONLY'
                : 'INFO'
  );
  return <span className={`cmd-status is-${kind}`}>{text}</span>;
}

function PostRecordCell({ row }: { row: CommandRow }) {
  const [imageFailed, setImageFailed] = useState(false);
  const raw = row.raw;
  const handle = str(raw.feeder_handle).replace(/^@/, '');
  const postKey = str(raw.post_key, row.subtitle);
  const checkpoint = labelize(raw.checkpoint, row.eyebrow);
  const mediaType = str(raw.media_type).toLowerCase();
  const thumbnailUrl = str(raw.thumbnail_url);
  const hasMetrics = ['views', 'likes', 'comments'].some((key) => raw[key] != null);
  const FallbackIcon = mediaType === 'video' || mediaType === 'reel' ? Video : ImageIcon;

  return (
    <span className="cmd-post-cell">
      <span className="cmd-post-thumb">
        {thumbnailUrl && !imageFailed ? (
          <img src={thumbnailUrl} alt="" loading="lazy" onError={() => setImageFailed(true)} />
        ) : (
          <FallbackIcon size={18} />
        )}
      </span>
      <span className="cmd-post-copy">
        <strong>{handle ? `@${handle}` : row.title}</strong>
        <code>{postKey}</code>
        <span className="cmd-post-meta">
          <em>{checkpoint}</em>
        </span>
        {hasMetrics ? (
          <span className="cmd-post-metrics">
            <small>{formatNumber(raw.views)} views</small>
            <small>{formatNumber(raw.likes)} likes</small>
            <small>{formatNumber(raw.comments)} comments</small>
          </span>
        ) : null}
      </span>
    </span>
  );
}

function opsStatus(health: unknown): StatusKind {
  const key = str(health).toLowerCase();
  if (key === 'smooth') return 'live';
  if (key === 'pending') return 'partial';
  if (key === 'failed') return 'failed';
  if (key === 'stale') return 'stale';
  if (key === 'missing') return 'missing';
  return 'info';
}

function opsEventTime(raw: Dict, time: OpsListTime): string | null {
  if (time === 'upcoming') return str(raw.nextRunAt) || str(raw.happenedAt) || null;
  return str(raw.happenedAt) || str(raw.nextRunAt) || null;
}

function rowFromOpsEvent(raw: Dict, time: OpsListTime = 'recent'): CommandRow {
  return makeRow(raw, {
    id: str(raw.id, crypto.randomUUID()),
    title: str(raw.title, 'Ops event'),
    eyebrow: str(raw.source, 'feedOps'),
    subtitle: [str(raw.feedName), str(raw.feederHandle) ? `@${str(raw.feederHandle)}` : '', str(raw.detail)].filter(Boolean).join(' / '),
    status: str(raw.status, 'done'),
    metric: str(raw.kind, str(raw.source, 'event')),
    updatedAt: opsEventTime(raw, time),
    source: str(raw.source, 'feedOps'),
  });
}

function FeederOpsPill({ feeder }: { feeder: Dict }) {
  return (
    <span className={`cmd-feeder-pill is-${str(feeder.health, 'missing')}`}>
      <i className={`cmd-dot is-${opsStatus(feeder.health)}`} />
      <strong>@{str(feeder.handle, 'unknown')}</strong>
      <em>{str(feeder.role, 'standard')}</em>
      <small>{formatNumber(feeder.pendingCount)}p / {formatNumber(feeder.failureCount)}f</small>
    </span>
  );
}

function OpsEventRow({ event, onSelect, time }: { event: Dict; onSelect: (row: CommandRow) => void; time: OpsListTime }) {
  const [imageFailed, setImageFailed] = useState(false);
  const thumbnailUrl = str(event.thumbnailUrl);

  return (
    <button type="button" className="cmd-ops-event" onClick={() => onSelect(rowFromOpsEvent(event, time))}>
      <span className="cmd-post-thumb">
        {thumbnailUrl && !imageFailed ? (
          <img src={thumbnailUrl} alt="" loading="lazy" onError={() => setImageFailed(true)} />
        ) : (
          <ImageIcon size={17} />
        )}
      </span>
      <span className="cmd-ops-event-copy">
        <strong>{str(event.feedName, 'Unowned feed')} {str(event.feederHandle) ? `/ @${str(event.feederHandle)}` : ''}</strong>
        <small>{str(event.title, labelize(event.kind, 'Event'))} / {str(event.source, 'source')}</small>
        <em>{str(event.detail, str(event.postKey, 'No detail'))}</em>
      </span>
      <span className="cmd-ops-event-state">
        <StatusChip status={str(event.status, 'done')} />
        <small>{relativeTime(opsEventTime(event, time))}</small>
      </span>
    </button>
  );
}

function OpsEventList({
  title,
  events,
  empty,
  onSelect,
  time = 'recent',
}: {
  title: string;
  events: Dict[];
  empty: string;
  onSelect: (row: CommandRow) => void;
  time?: OpsListTime;
}) {
  return (
    <section className="cmd-panel cmd-ops-list">
      <div className="cmd-panel-head">
        <span>{title}</span>
        <strong>{events.length}</strong>
      </div>
      {events.length ? events.map((event) => (
        <OpsEventRow key={str(event.id)} event={event} onSelect={onSelect} time={time} />
      )) : (
        <div className="cmd-empty small">
          <ShieldCheck size={18} />
          <strong>{empty}</strong>
        </div>
      )}
    </section>
  );
}

function FeedOpsCard({ feed, onSelect }: { feed: Dict; onSelect: (row: CommandRow) => void }) {
  const latestFailure = dict(feed.latestFailure);
  return (
    <section className={`cmd-feed-card is-${str(feed.health, 'missing')}`}>
      <div className="cmd-feed-card-head">
        <div>
          <span>FEED</span>
          <strong>{str(feed.feedName, 'Untitled feed')}</strong>
        </div>
        <StatusChip status={opsStatus(feed.health)} label={str(feed.health, 'missing').toUpperCase()} />
      </div>
      <div className="cmd-feed-stats">
        <div><span>feeders</span><strong>{formatNumber(feed.activeFeederCount)} / {formatNumber(feed.feederCount)}</strong></div>
        <div><span>last</span><strong>{relativeTime(feed.lastActivityAt)}</strong></div>
        <div><span>next</span><strong>{relativeTime(feed.nextWorkAt)}</strong></div>
      </div>
      <div className="cmd-feeder-pills">
        {list(feed.feeders).slice(0, 10).map((feeder) => <FeederOpsPill key={str(feeder.feederId, str(feeder.handle))} feeder={feeder} />)}
      </div>
      {latestFailure.id ? (
        <button type="button" className="cmd-feed-failure" onClick={() => onSelect(rowFromOpsEvent(latestFailure))}>
          <AlertTriangle size={14} />
          <span>{str(latestFailure.title, 'Latest failure')}</span>
          <small>{relativeTime(latestFailure.happenedAt)}</small>
        </button>
      ) : null}
    </section>
  );
}

function FeedOpsBoard({ feeds, onSelect }: { feeds: Dict[]; onSelect: (row: CommandRow) => void }) {
  return (
    <section className="cmd-feed-board">
      {feeds.length ? feeds.map((feed) => (
        <FeedOpsCard key={str(feed.feedId, str(feed.feedName))} feed={feed} onSelect={onSelect} />
      )) : (
        <div className="cmd-empty">
          <Boxes size={22} />
          <strong>No feeds in scope</strong>
          <span>The command API did not return feed operations rows.</span>
        </div>
      )}
    </section>
  );
}

function runwayGroupKey(event: Dict) {
  const feed = str(event.feedId, str(event.feedName, 'unowned'));
  const feeder = str(event.feederId, str(event.feederHandle, 'all'));
  return `${feed}:${feeder}:${str(event.source, 'source')}`;
}

function groupRunwayEvents(events: Dict[]): RunwayGroup[] {
  const grouped = new Map<string, Dict[]>();
  events.forEach((event) => {
    const key = runwayGroupKey(event);
    grouped.set(key, [...(grouped.get(key) || []), event]);
  });
  return Array.from(grouped.entries()).map(([id, members]) => ({
    id,
    feedName: str(members[0]?.feedName, 'UNOWNED'),
    feederHandle: str(members[0]?.feederHandle),
    events: members,
    first: members[0],
  }));
}

function runwayToneForEvent(event: Dict): RunwayTone {
  const queueState = str(event.queueState).toLowerCase();
  if (queueState === 'overdue') return 'overdue';
  if (queueState === 'in_progress') return 'running';
  if (queueState === 'scheduled') {
    const at = Date.parse(commandEventTime(event as CommandEvent) || '');
    return Number.isFinite(at) && at - Date.now() > 12 * 60 * 60 * 1000 ? 'later' : 'due';
  }
  return queueState === 'queued' ? 'due' : 'later';
}

function formatRunwayDelta(value: unknown, prefix: 'due' | 'overdue') {
  const at = Date.parse(str(value));
  if (!Number.isFinite(at)) return prefix === 'due' ? 'QUEUED' : 'OVERDUE';
  const minutes = Math.max(0, Math.round(Math.abs(at - Date.now()) / 60_000));
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor((minutes % 1440) / 60);
  const remainder = minutes % 60;
  const amount = days
    ? `${days}d ${hours}h`
    : hours
      ? `${hours}h ${remainder}m`
      : `${remainder}m`;
  return prefix === 'overdue' ? `OVERDUE ${amount}` : `DUE IN ${amount}`;
}

function runwayStateLabel(event: Dict, tone = runwayToneForEvent(event)) {
  if (tone === 'overdue') return formatRunwayDelta(commandEventTime(event as CommandEvent), 'overdue');
  if (tone === 'running') {
    const status = str(event.status, 'running').toUpperCase().replaceAll('_', ' ');
    return status === 'RUNNING' ? 'RUNNING NOW' : status;
  }
  if (str(event.queueState).toLowerCase() === 'queued') return 'QUEUED';
  return formatRunwayDelta(commandEventTime(event as CommandEvent), 'due');
}

function RunwayThumbnail({ url }: { url: string }) {
  const [failed, setFailed] = useState(false);
  return (
    <span className="cmd-runway-thumb">
      {url && !failed ? (
        <img src={url} alt="" loading="lazy" onError={() => setFailed(true)} />
      ) : (
        <ImageIcon size={14} aria-hidden="true" />
      )}
    </span>
  );
}

function RunwayThumbStack({ events }: { events: Dict[] }) {
  const urls = Array.from(new Set(events.map((event) => str(event.thumbnailUrl)).filter(Boolean)));
  const visible = urls.slice(0, 3);
  return (
    <span className="cmd-runway-thumbs" aria-hidden="true">
      {visible.length ? visible.map((url) => <RunwayThumbnail key={url} url={url} />) : <RunwayThumbnail url="" />}
      {events.length > visible.length ? <small>+{events.length - visible.length}</small> : null}
    </span>
  );
}

function CheckpointTrace({ event }: { event: Dict }) {
  const checkpoint = `${str(event.kind)} ${str(event.title)}`.toLowerCase();
  const steps = ['d1', 'd3', 'd7', 'd21'];
  const currentStep = steps.findIndex((step) => checkpoint.includes(step));
  if (currentStep < 0) {
    return (
      <span className="cmd-running-trace is-activity" aria-label="Job is actively running">
        <i />
      </span>
    );
  }
  return (
    <span className="cmd-running-trace" aria-label={`${steps[currentStep].toUpperCase()} in the D1, D3, D7, D21 checkpoint sequence`}>
      {steps.map((step, index) => <i key={step} className={index <= currentStep ? 'is-complete' : ''} />)}
    </span>
  );
}

function RunwayStateBadge({ event, tone }: { event: Dict; tone: RunwayTone }) {
  return <span className={`cmd-runway-state is-${tone}`}>{runwayStateLabel(event, tone)}</span>;
}

function RunwayCard({
  group,
  tone,
  selected,
  index,
  reduced,
  onSelect,
}: {
  group: RunwayGroup;
  tone: RunwayTone;
  selected: CommandRow | null;
  index: number;
  reduced: boolean;
  onSelect: (row: CommandRow) => void;
}) {
  const isSelected = Boolean(selected && group.events.some((event) => (
    str(event.id) === selected.id && str(event.source) === selected.source
  )));
  const checkpoints = new Set(group.events.map((event) => str(event.kind, str(event.title))).filter(Boolean)).size;
  const title = group.feedName.toUpperCase();
  const handle = group.feederHandle ? `@${group.feederHandle.replace(/^@/, '')}` : 'UNASSIGNED';

  return (
    <motion.button
      type="button"
      className={`cmd-runway-card is-${tone} ${isSelected ? 'is-selected' : ''}`}
      initial={reduced ? false : { opacity: 0, transform: 'translate3d(0px, 10px, 0px) scale(0.985)' }}
      animate={{ opacity: 1, transform: commandRestingTransform }}
      transition={reduced ? { duration: 0.1 } : {
        opacity: { duration: 0.18, ease: commandEaseOut, delay: Math.min(index * 0.035, 0.14) },
        transform: { ...commandEnterSpring, delay: Math.min(index * 0.035, 0.14) },
      }}
      whileHover={reduced ? undefined : { transform: 'translate3d(0px, -2px, 0px) scale(1)' }}
      whileTap={reduced ? undefined : { transform: 'translate3d(0px, 0px, 0px) scale(0.97)' }}
      onClick={() => onSelect(rowFromOpsEvent(group.first, 'upcoming'))}
      aria-pressed={isSelected}
      aria-label={`${title}, ${handle}, ${runwayStateLabel(group.first, tone)}, ${group.events.length} jobs`}
    >
      <RunwayThumbStack events={group.events} />
      <span className="cmd-runway-card-copy">
        <strong>{title} <b>/</b> {handle}</strong>
        <small>{str(group.first.title, labelize(group.first.kind, 'Job'))}</small>
        <RunwayStateBadge event={group.first} tone={tone} />
        {tone === 'running' ? <CheckpointTrace event={group.first} /> : null}
        <span className="cmd-runway-card-meta">
          <em>{checkpoints} {checkpoints === 1 ? 'checkpoint' : 'checkpoints'}</em>
          <em>{group.events.length} {group.events.length === 1 ? 'job' : 'jobs'}</em>
        </span>
      </span>
    </motion.button>
  );
}

function RunwayLane({
  label,
  tone,
  events,
  selected,
  expanded,
  reduced,
  onSelect,
}: {
  label: string;
  tone: RunwayTone;
  events: Dict[];
  selected: CommandRow | null;
  expanded: boolean;
  reduced: boolean;
  onSelect: (row: CommandRow) => void;
}) {
  const groups = groupRunwayEvents(events);
  const visible = expanded ? groups : groups.slice(0, tone === 'running' ? 1 : 3);
  const hidden = groups.length - visible.length;
  return (
    <motion.section
      layout="position"
      className={`cmd-runway-lane is-${tone}`}
      transition={reduced ? { duration: 0 } : { layout: commandLayoutSpring }}
      aria-labelledby={`cmd-runway-${tone}`}
    >
      <div className="cmd-lane-label">
        <span id={`cmd-runway-${tone}`}>{label}</span>
        <strong>{events.length ? formatNumber(events.length) : '0'}</strong>
        <small>{events.length === 1 ? 'job' : 'jobs'}</small>
      </div>
      <div className={`cmd-lane-track ${tone === 'running' ? 'is-running' : ''}`}>
        {visible.length ? (
          <AnimatePresence initial={false}>
            {visible.map((group, index) => (
              <motion.div
                key={group.id}
                layout="position"
                className={`cmd-runway-card-slot is-${tone}`}
                initial={reduced ? false : { opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{
                  opacity: 0,
                  transition: { duration: reduced ? 0.08 : 0.14, ease: commandEaseOut },
                }}
                transition={reduced ? { duration: 0 } : { layout: commandLayoutSpring }}
              >
                <RunwayCard
                  group={group}
                  tone={tone}
                  selected={selected}
                  index={index}
                  reduced={reduced}
                  onSelect={onSelect}
                />
              </motion.div>
            ))}
          </AnimatePresence>
        ) : (
          <div className="cmd-runway-empty">
            <ShieldCheck size={16} aria-hidden="true" />
            <span>{tone === 'overdue' ? 'No overdue work' : tone === 'running' ? 'No jobs running' : 'Nothing scheduled here'}</span>
          </div>
        )}
        <AnimatePresence initial={false}>
          {hidden > 0 ? (
            <motion.span
              key="more"
              className="cmd-runway-more"
              initial={reduced ? false : { opacity: 0, transform: 'translate3d(0px, 3px, 0px)' }}
              animate={{ opacity: 1, transform: 'translate3d(0px, 0px, 0px)' }}
              exit={reduced
                ? { opacity: 0 }
                : { opacity: 0, transform: 'translate3d(0px, -2px, 0px)' }}
              transition={reduced ? { duration: 0.1 } : { duration: 0.18, ease: commandEaseOut }}
            >
              +{hidden} grouped lanes
            </motion.span>
          ) : null}
        </AnimatePresence>
      </div>
    </motion.section>
  );
}

function RunwayMetrics({
  total,
  overdue,
  running,
  dueSoon,
  revision,
  sampled,
}: {
  total: number;
  overdue: number;
  running: number;
  dueSoon: number;
  revision: number;
  sampled: boolean;
}) {
  const metrics = [
    { label: 'OPEN JOBS', value: `${formatNumber(total)}${sampled ? '+' : ''}`, icon: Clock3, tone: 'overdue' },
    { label: 'OVERDUE', value: formatNumber(overdue), icon: Clock3, tone: 'overdue' },
    { label: 'RUNNING NOW', value: formatNumber(running), icon: Play, tone: 'running' },
    { label: 'DUE IN 12H', value: formatNumber(dueSoon), icon: Clock3, tone: 'due' },
  ];
  return (
    <section className="cmd-runway-metrics" aria-label="Open work summary">
      <div className="cmd-runway-metric-grid">
        {metrics.map((metric) => {
          const Icon = metric.icon;
          return (
            <div key={metric.label} className={`cmd-runway-metric is-${metric.tone}`}>
              <Icon size={38} strokeWidth={1.65} aria-hidden="true" />
              <span>
                <RollingNumber value={metric.value} revision={revision} />
                <small>{metric.label}</small>
              </span>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function CommandViewToolbar({
  active,
  exploreActive,
  query,
  statusFilter,
  statuses,
  total,
  sampled,
  expanded,
  onQuery,
  onStatusFilter,
  onToggleExpanded,
  onNavigate,
  motionProps,
}: {
  active: DomainId;
  exploreActive: boolean;
  query: string;
  statusFilter: string;
  statuses: string[];
  total: number;
  sampled: boolean;
  expanded: boolean;
  onQuery: (value: string) => void;
  onStatusFilter: (value: string) => void;
  onToggleExpanded: () => void;
  onNavigate: (domain: DomainId, motion?: NavigationMotion) => void;
  motionProps: CommandRegionMotionProps;
}) {
  const context = active === 'overview'
    ? ['NOW', 'Live operating picture']
    : active === 'pending'
      ? ['NEXT', 'Time-ordered open work']
      : ['HISTORY', 'Recent completed activity'];

  return (
    <motion.section
      className={`cmd-view-toolbar ${exploreActive ? 'is-explore' : ''}`}
      data-command-motion="controls"
      aria-label="View controls"
      {...motionProps}
    >
      {exploreActive ? (
        <nav key="explore-nav" className="cmd-explore-strip" aria-label="Explore command domains">
          {exploreNavItems.map((item) => {
            const Icon = item.icon;
            return (
              <button
                key={item.id}
                type="button"
                className={active === item.id ? 'is-active' : ''}
                onClick={(event) => onNavigate(item.id, event.detail === 0 ? 'instant' : 'fluid')}
                aria-current={active === item.id ? 'page' : undefined}
              >
                <Icon size={14} strokeWidth={1.8} aria-hidden="true" />
                <span>{item.label}</span>
              </button>
            );
          })}
        </nav>
      ) : (
        <div key={active} className="cmd-view-context">
          <strong>{context[0]}</strong>
          <span>{context[1]}</span>
        </div>
      )}

      <div key={`actions:${active}`} className="cmd-view-actions">
        {!exploreActive ? (
          <label className="cmd-toolbar-search">
            <Search size={14} aria-hidden="true" />
            <input
              value={query}
              onChange={(event) => onQuery(event.target.value)}
              placeholder={active === 'pending' ? 'Search open jobs' : 'Search this view'}
              aria-label={`Search ${context[0].toLowerCase()}`}
            />
          </label>
        ) : null}
        {exploreActive ? (
          <label className="cmd-toolbar-filter">
            <span>{active === 'feeds' ? 'Health' : 'Status'}</span>
            <select value={statusFilter} onChange={(event) => onStatusFilter(event.target.value)}>
              {statuses.map((status) => <option key={status} value={status}>{labelize(status)}</option>)}
            </select>
          </label>
        ) : null}
        {active === 'pending' ? (
          <button type="button" className="cmd-toolbar-action" onClick={onToggleExpanded} aria-expanded={expanded}>
            <span>{expanded ? 'COLLAPSE' : 'VIEW ALL'} {formatNumber(total)}{sampled ? '+' : ''}</span>
            <ChevronRight size={16} aria-hidden="true" />
          </button>
        ) : null}
      </div>
    </motion.section>
  );
}

function TemporalRunway({
  overdue,
  running,
  dueNext,
  later,
  selected,
  expanded,
  reduced,
  onSelect,
}: {
  overdue: Dict[];
  running: Dict[];
  dueNext: Dict[];
  later: Dict[];
  selected: CommandRow | null;
  expanded: boolean;
  reduced: boolean;
  onSelect: (row: CommandRow) => void;
}) {
  const [clock, setClock] = useState(() => new Date());
  useEffect(() => {
    const timer = window.setInterval(() => setClock(new Date()), 60_000);
    return () => window.clearInterval(timer);
  }, []);
  const timeParts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(clock);
  const hour = Number(timeParts.find((part) => part.type === 'hour')?.value || 9);
  const minute = Number(timeParts.find((part) => part.type === 'minute')?.value || 0);
  const nowPosition = Math.max(0, Math.min(100, (((hour + minute / 60) - 9) / 12) * 100));
  const runwayStyle = { '--cmd-now-position': `${nowPosition}%` } as CSSProperties;

  return (
    <section className="cmd-runway" style={runwayStyle} aria-label="Open work timeline">
      <div className="cmd-time-axis" aria-hidden="true">
        <span />
        <div>
          {['09:00', '11:00', '13:00', '15:00', '17:00', '19:00', '21:00+'].map((label) => <small key={label}>{label}</small>)}
          <b>NOW</b>
        </div>
      </div>
      <div className="cmd-now-line" aria-hidden="true"><i /></div>
      <RunwayLane label="OVERDUE" tone="overdue" events={overdue} selected={selected} expanded={expanded} reduced={reduced} onSelect={onSelect} />
      <RunwayLane label="RUNNING NOW" tone="running" events={running} selected={selected} expanded={expanded} reduced={reduced} onSelect={onSelect} />
      <RunwayLane label="DUE NEXT" tone="due" events={dueNext} selected={selected} expanded={expanded} reduced={reduced} onSelect={onSelect} />
      <RunwayLane label="LATER" tone="later" events={later} selected={selected} expanded={expanded} reduced={reduced} onSelect={onSelect} />
      <footer>Times use the operational claimable schedule · refresh manually for the latest state</footer>
    </section>
  );
}

function RunwayInspector({
  payload,
  selected,
  related,
  reduced,
  motionProps,
}: {
  payload: CommandPayload;
  selected: CommandRow | null;
  related: Dict[];
  reduced: boolean;
  motionProps: CommandRegionMotionProps;
}) {
  const [expandedRecordKey, setExpandedRecordKey] = useState<string | null>(null);
  const [copiedRecordKey, setCopiedRecordKey] = useState<string | null>(null);
  const selectedRecordKey = selected ? `${selected.source}:${selected.id}` : null;
  const showRaw = Boolean(selectedRecordKey && expandedRecordKey === selectedRecordKey);
  const copied = Boolean(selectedRecordKey && copiedRecordKey === selectedRecordKey);
  const raw = selected?.raw || {};
  const tone = runwayToneForEvent(raw);
  const access = dict(payload.access);
  const entries = selected ? [
    ['Status', str(raw.status, selected.status)],
    ['Checkpoint', str(raw.kind, selected.metric)],
    ['Source', selected.source],
    ['Job ID', selected.id],
    ['Feed ID', str(raw.feedId, '—')],
    ['Feeder ID', str(raw.feederId, '—')],
    ['Feeder handle', str(raw.feederHandle, '—')],
    ['Post key', str(raw.postKey, '—')],
    ['Updated', shortDate(raw.happenedAt || selected.updatedAt)],
    ['Actionable', shortDate(commandEventTime(raw as CommandEvent))],
  ] : [];

  const copyRecord = useCallback(async () => {
    if (!selected || !selectedRecordKey) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(selected.raw, null, 2));
      setCopiedRecordKey(selectedRecordKey);
    } catch {
      setCopiedRecordKey(null);
    }
  }, [selected, selectedRecordKey]);

  return (
    <motion.aside
      className="cmd-runway-inspector"
      data-command-motion="inspector"
      aria-label="Selected job"
      {...motionProps}
    >
      <div className="cmd-runway-inspector-head">
        <span>SELECTED JOB</span>
        {selected ? <RunwayStateBadge event={raw} tone={tone} /> : <span className="cmd-runway-state is-later">NONE</span>}
      </div>
      <div className="cmd-inspector-body-stack">
        <AnimatePresence initial={false} mode="sync">
          {selected ? (
            <motion.div
              key={`${selected.source}:${selected.id}`}
              className="cmd-runway-inspector-body"
              initial={reduced ? { opacity: 0.96 } : {
                opacity: 0.98,
                transform: 'translate3d(0px, 4px, 0px) scale(0.997)',
              }}
              animate={{ opacity: 1, transform: commandRestingTransform }}
              exit={reduced ? { opacity: 0.06 } : {
                opacity: 0,
                transform: 'translate3d(0px, -2px, 0px) scale(0.998)',
                transition: {
                  opacity: { duration: 0.1, ease: commandEaseOut },
                  transform: { duration: 0.12, ease: commandEaseOut },
                },
              }}
              transition={reduced ? { duration: 0.1 } : {
                opacity: { duration: 0.16, ease: commandEaseOut },
                transform: { duration: 0.2, ease: commandEaseOut },
              }}
            >
            <h2>{selected.title}</h2>
            <p>{str(raw.feedName, 'UNOWNED').toUpperCase()} / {str(raw.feederHandle) ? `@${str(raw.feederHandle).replace(/^@/, '')}` : 'UNASSIGNED'}</p>
            <button
              type="button"
              className="cmd-runway-inspect-button"
              onClick={() => setExpandedRecordKey((current) => current === selectedRecordKey ? null : selectedRecordKey)}
              aria-expanded={showRaw}
            >
              <Search size={17} aria-hidden="true" />
              <span>{showRaw ? 'HIDE RAW RECORD' : 'INSPECT JOB'}</span>
              <ChevronRight className={showRaw ? 'is-open' : ''} size={15} aria-hidden="true" />
            </button>
            <div className="cmd-runway-detail-list">
              {entries.map(([label, value], index) => (
                <motion.div
                  key={label}
                  initial={reduced ? false : { opacity: 0, transform: 'translate3d(0px, 5px, 0px)' }}
                  animate={{ opacity: 1, transform: 'translate3d(0px, 0px, 0px)' }}
                  transition={reduced ? { duration: 0 } : {
                    duration: 0.16,
                    delay: Math.min(index * 0.018, 0.072),
                    ease: commandEaseOut,
                  }}
                >
                  <span>{label}</span>
                  <strong>{value}</strong>
                </motion.div>
              ))}
            </div>
            <AnimatePresence initial={false}>
              {showRaw ? (
                <motion.div
                  key="raw-record"
                  className="cmd-runway-raw"
                  initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0px, -4px, 0px) scale(0.997)' }}
                  animate={{ opacity: 1, transform: commandRestingTransform }}
                  exit={reduced ? { opacity: 0 } : {
                    opacity: 0,
                    transform: 'translate3d(0px, -2px, 0px) scale(0.998)',
                    transition: {
                      opacity: { duration: 0.1, ease: commandEaseOut },
                      transform: { duration: 0.12, ease: commandEaseOut },
                    },
                  }}
                  transition={reduced ? { duration: 0.1 } : {
                    opacity: { duration: 0.16, ease: commandEaseOut },
                    transform: { duration: 0.2, ease: commandEaseOut },
                  }}
                >
                  <button type="button" onClick={copyRecord}>
                    {copied ? <Check size={13} aria-hidden="true" /> : <Copy size={13} aria-hidden="true" />}
                    {copied ? 'COPIED' : 'COPY RECORD'}
                  </button>
                  <pre>{JSON.stringify(raw, null, 2)}</pre>
                </motion.div>
              ) : null}
            </AnimatePresence>
            <motion.div layout="position" transition={reduced ? { duration: 0 } : { layout: commandLayoutSpring }} className="cmd-runway-related">
              <span>JOBS IN THIS GROUP</span>
              <div>
                {related.slice(0, 6).map((event) => (
                  <RunwayThumbnail key={str(event.id)} url={str(event.thumbnailUrl)} />
                ))}
                {related.length > 6 ? <small>+{related.length - 6}</small> : null}
              </div>
            </motion.div>
            </motion.div>
          ) : (
            <motion.div
              key="empty"
              className="cmd-runway-inspector-empty"
              initial={reduced
                ? { opacity: 0.96 }
                : { opacity: 0.98, transform: 'translate3d(0px, 4px, 0px) scale(0.997)' }}
              animate={{ opacity: 1, transform: commandRestingTransform }}
              exit={reduced
                ? { opacity: 0 }
                : { opacity: 0, transform: 'translate3d(0px, -2px, 0px) scale(0.998)' }}
              transition={{ duration: reduced ? 0.1 : 0.16, ease: commandEaseOut }}
            >
              <Boxes size={22} aria-hidden="true" />
              <strong>Select a job group</strong>
              <span>Its source and timing will appear here.</span>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
      <motion.p
        layout="position"
        transition={reduced ? { duration: 0 } : { layout: commandLayoutSpring }}
        className="cmd-runway-access-note"
      >
        {str(access.note, 'Read-only command scope. No mutation controls are exposed.')}
      </motion.p>
    </motion.aside>
  );
}

function PortalHeader({
  payload,
  betaState,
  criticalCount,
}: {
  payload: CommandPayload | null;
  betaState: 'READY' | 'WATCH' | 'BLOCKED';
  criticalCount: number;
}) {
  const readAt = str(payload?.generatedAt);

  return (
    <header className="cmd-portal-header" aria-label={`${betaState} command state with ${criticalCount} critical surfaces`}>
      <div className="cmd-portal-title">
        <h1>COMMAND CENTER</h1>
        <span>FEEDME READ-ONLY LIVE OPS</span>
      </div>
      <div className="cmd-header-live">
        <span className="cmd-live-read"><i /> LIVE READ</span>
        <span>· {readAt ? shortDate(readAt).toUpperCase() : 'READ PENDING'}</span>
        <span>· {readAt ? shortDate(readAt, true).split(', ').pop() : '—'} IST</span>
        <StatusChip status="readonly" />
      </div>
    </header>
  );
}

function CommandSyncBar({
  refreshing,
  lastLoadedAt,
  refreshError,
  onRefresh,
}: {
  refreshing: boolean;
  lastLoadedAt: string | null;
  refreshError: LoadFailure | null;
  onRefresh: () => void;
}) {
  const state: StatusKind = refreshError ? 'failed' : refreshing ? 'partial' : 'live';
  const label = refreshError ? 'REFRESH FAILED' : refreshing ? 'REFRESHING' : 'SNAPSHOT';
  const detail = refreshError
    ? refreshError.message
    : lastLoadedAt
      ? `Loaded ${relativeTime(lastLoadedAt)} / ${shortDate(lastLoadedAt, true)}`
      : 'Waiting for the first load.';

  return (
    <section className="cmd-sync-bar" aria-live="polite" aria-atomic="true">
      <div>
        <StatusChip status={state} label={label} />
        <span className="cmd-sync-detail">{detail}</span>
      </div>
      <button
        type="button"
        className="cmd-refresh-button"
        onClick={onRefresh}
        disabled={refreshing}
        aria-label={refreshing ? 'Refreshing command snapshot' : refreshError ? 'Retry command snapshot' : 'Refresh command snapshot'}
        title={refreshing ? 'Refreshing' : refreshError ? 'Retry snapshot' : 'Refresh snapshot'}
      >
        <RefreshCw size={14} aria-hidden="true" />
        <span>{refreshing ? 'Refreshing' : refreshError ? 'Retry' : 'Refresh now'}</span>
      </button>
    </section>
  );
}

function BetaReadinessBand({
  betaState,
  criticalCount,
  attention,
}: {
  betaState: 'READY' | 'WATCH' | 'BLOCKED';
  criticalCount: number;
  attention: Attention[];
}) {
  const lead = attention[0];
  const issueSummary = criticalCount
    ? `${criticalCount} critical ${criticalCount === 1 ? 'surface is' : 'surfaces are'} blocking beta.`
    : attention.length
      ? `${attention.length} ${attention.length === 1 ? 'surface needs' : 'surfaces need'} review before beta lock.`
      : 'All watched surfaces are clear in this read.';
  const leadText = lead ? `${lead.label} / ${lead.detail}` : 'No failed, stale, or missing surfaces detected in this read.';

  return (
    <section className={`cmd-readiness is-${betaState.toLowerCase()}`}>
      <div>
        <span>READ-ONLY BETA STATE</span>
        <strong>{betaState}</strong>
        <p><b>{issueSummary}</b> {leadText}</p>
      </div>
      <div className="cmd-readiness-count">
        <span>{criticalCount}</span>
        <small>{criticalCount === 1 ? 'blocker' : 'blockers'}</small>
      </div>
      <div className="cmd-band-sweep" aria-hidden="true" />
    </section>
  );
}

function SystemHealthMatrix({
  systems,
  runtime,
  active,
  onNavigate,
}: {
  systems: SystemHealth[];
  runtime: RuntimeSurface[];
  active: DomainId;
  onNavigate: (domain: DomainId, motion?: NavigationMotion) => void;
}) {
  return (
    <section className="cmd-panel" aria-labelledby="cmd-system-matrix-title">
      <div className="cmd-panel-head">
        <span id="cmd-system-matrix-title">SYSTEM HEALTH MATRIX</span>
        <strong>{systems.length + runtime.length} surfaces</strong>
      </div>
      <div className="cmd-system-grid">
        {systems.map((system) => (
          <button
            key={system.id}
            type="button"
            className={`cmd-system-tile ${active === system.id ? 'is-active' : ''}`}
            onClick={(event) => onNavigate(system.id, event.detail === 0 ? 'instant' : 'fluid')}
            aria-label={`${system.label}: ${system.value}, ${system.note}`}
          >
            <div>
              <span>{system.label}</span>
              <StatusChip status={system.status} label={neutralDomains.has(system.id) ? 'NOT CONNECTED' : undefined} />
            </div>
            <strong>{system.value}</strong>
            <small>{system.note}</small>
            <small>{system.updatedAt ? `Updated ${relativeTime(system.updatedAt)}` : 'No source timestamp'}</small>
          </button>
        ))}
        {runtime.map((surface) => (
          <article key={surface.id} className="cmd-system-tile cmd-runtime-tile" aria-label={`${surface.label}: ${surface.value}, ${surface.note}`}>
            <div>
              <span>{surface.label}</span>
              <StatusChip status={surface.status} />
            </div>
            <strong>{surface.value}</strong>
            <small>{surface.note}</small>
            <small>{surface.updatedAt ? `Updated ${relativeTime(surface.updatedAt)}` : 'Awaiting runtime telemetry'}</small>
          </article>
        ))}
      </div>
    </section>
  );
}

function AttentionQueue({
  attention,
  onNavigate,
  onSelect,
}: {
  attention: Attention[];
  onNavigate: (domain: DomainId, motion?: NavigationMotion) => void;
  onSelect: (row: CommandRow) => void;
}) {
  return (
    <section className="cmd-panel" aria-labelledby="cmd-attention-title">
      <div className="cmd-panel-head">
        <span id="cmd-attention-title">FULL ATTENTION QUEUE</span>
        <strong>{attention.length}</strong>
      </div>
      {attention.length ? attention.map((item) => (
        <button
          key={item.id}
          type="button"
          className="cmd-attention-row"
          onClick={(event) => {
            if (item.domain) onNavigate(item.domain, event.detail === 0 ? 'instant' : 'fluid');
            if (item.row) onSelect(item.row);
          }}
        >
          <StatusChip status={item.status} />
          <span>
            <strong>{item.label}</strong>
            <small>{item.detail} / {item.source}</small>
          </span>
          <em>{relativeTime(item.updatedAt)}</em>
        </button>
      )) : (
        <div className="cmd-empty small">
          <ShieldCheck size={18} />
          <strong>No surfaces need attention</strong>
        </div>
      )}
    </section>
  );
}

function NotConnectedNotice({ domain }: { domain: 'intelligence' | 'finance' }) {
  return (
    <section className="cmd-panel cmd-not-connected" role="note">
      <div className="cmd-panel-head">
        <span>{domain.toUpperCase()} CONNECTION</span>
        <StatusChip status="readonly" label="NOT CONNECTED" />
      </div>
      <p>
        {labelize(domain)} is intentionally excluded from Command readiness until its production contract is connected.
        Any records below are source evidence only and do not claim live operational coverage.
      </p>
    </section>
  );
}

function ProductSurfaceRegistry({ surfaces }: { surfaces: Dict[] }) {
  if (!surfaces.length) return null;
  return (
    <section className="cmd-panel" aria-labelledby="cmd-product-surfaces-title">
      <div className="cmd-panel-head">
        <span id="cmd-product-surfaces-title">PRODUCT SURFACES</span>
        <strong>{surfaces.length}</strong>
      </div>
      <div className="cmd-surface-grid">
        {surfaces.map((surface) => {
          const lifecycle = str(surface.lifecycle, 'unknown');
          const dataMode = str(surface.dataMode, 'unknown');
          const sourceStatus = str(dict(surface.sourceProbe).status, 'unknown');
          const status: StatusKind = sourceStatus === 'unavailable'
            ? 'failed'
            : lifecycle === 'live' && dataMode === 'real' && sourceStatus === 'available'
              ? 'live'
              : lifecycle === 'preview'
                ? 'readonly'
                : 'partial';
          return (
            <a key={str(surface.id, str(surface.route))} href={str(surface.route, '#')} className="cmd-surface-card">
              <span>
                <strong>{str(surface.label, labelize(surface.id))}</strong>
                <ExternalLink size={13} aria-hidden="true" />
              </span>
              <StatusChip status={status} label={lifecycle} />
              <small>{labelize(dataMode)} data / {labelize(sourceStatus)}</small>
              <em>{surface.gatesOperationalReadiness === true ? 'Readiness gate' : 'Informational'}</em>
            </a>
          );
        })}
      </div>
    </section>
  );
}

function pipelineMetrics(payload: CommandPayload, active: DomainId): PipelineMetric[] {
  const labels = pipelineLabels[active];
  const topline = dict(payload.topline);
  const feedOps = dict(payload.feedOps);
  const engine = dict(payload.engine);
  const engineTotals = dict(engine.totals);
  const engineStatuses = dict(engine.byStatus);
  const checkpoints = dict(payload.checkpoints);
  const checkpointStatuses = dict(checkpoints.jobsByCheckpoint);
  const fireTotals = dict(dict(payload.fireSignals).totals);
  const mediaTotals = dict(dict(payload.media).totals);
  const notificationTotals = dict(dict(payload.notifications).totals);
  const gaps = list(payload.instrumentationGaps);
  const feeds = list(feedOps.feeds);
  const recent = list(feedOps.recentActivity);
  const pending = list(feedOps.pendingAhead);
  const failures = list(feedOps.failures);
  const countDistinct = (items: Dict[], key: string) => new Set(items.map((item) => str(item[key])).filter(Boolean)).size;
  const fallbackRows = rowsForDomain(payload, active);
  const fallback = () => {
    const failed = fallbackRows.filter((row) => statusKind(row.status) === 'failed').length;
    const partial = fallbackRows.filter((row) => statusKind(row.status) === 'partial').length;
    const live = fallbackRows.filter((row) => statusKind(row.status) === 'live').length;
    return [live, partial, fallbackRows.length, failed].map((value, index) => ({
      label: labels[index],
      value: formatNumber(value),
      tone: index === 3 && value ? 'failed' as const : undefined,
    }));
  };

  if (active === 'account') {
    return [
      { label: labels[0], value: formatNumber(topline.users) },
      { label: labels[1], value: formatNumber(topline.feeds) },
      { label: labels[2], value: formatNumber(topline.feeders) },
      { label: labels[3], value: `${formatNumber(topline.contextCoveragePercent)}%` },
    ];
  }
  if (active === 'engine') {
    const values = [
      metricNumber(engineTotals, 'queued', statusCount(engineStatuses, 'pending', 'queued')),
      metricNumber(engineTotals, 'running', statusCount(engineStatuses, 'running')),
      metricNumber(engineTotals, 'retry', statusCount(engineStatuses, 'retry')),
      metricNumber(engineTotals, 'failed', statusCount(engineStatuses, 'failed', 'error')),
    ];
    return values.map((value, index) => ({ label: labels[index], value: formatNumber(value), tone: index === 3 && value ? 'failed' : undefined }));
  }
  if (active === 'checkpoints') {
    return ['d1', 'd3', 'd7', 'd21'].map((checkpoint, index) => ({
      label: labels[index],
      value: formatNumber(statusCount(checkpointStatuses, checkpoint)),
    }));
  }
  if (active === 'fire') {
    const values = [fireTotals.alerts, fireTotals.signals, fireTotals.hotPosts, fireTotals.staleOrSuppressedSignals];
    return values.map((value, index) => ({ label: labels[index], value: formatNumber(value), tone: index === 3 && num(value) ? 'stale' : undefined }));
  }
  if (active === 'media') {
    const values = [mediaTotals.activeAssets, mediaTotals.pendingCapture, mediaTotals.purgeQueue, mediaTotals.failed];
    return values.map((value, index) => ({ label: labels[index], value: formatNumber(value), tone: index === 3 && num(value) ? 'failed' : undefined }));
  }
  if (active === 'notifications') {
    const values = [notificationTotals.activeSubscriptions, notificationTotals.openJobs, notificationTotals.sentJobs, notificationTotals.failedJobs];
    return values.map((value, index) => ({ label: labels[index], value: formatNumber(value), tone: index === 3 && num(value) ? 'failed' : undefined }));
  }
  if (active === 'intelligence' || active === 'finance') {
    return labels.map((label, index) => ({
      label,
      value: index === 0 ? 'NOT CONNECTED' : '—',
      tone: index === 0 ? 'readonly' : undefined,
    }));
  }
  if (active === 'feeds') {
    const values = ['smooth', 'pending', 'failed', 'missing'].map((health) => feeds.filter((feed) => str(feed.health) === health).length);
    return values.map((value, index) => ({ label: labels[index], value: formatNumber(value), tone: index >= 2 && value ? index === 2 ? 'failed' : 'missing' : undefined }));
  }
  if (active === 'activity') {
    const values = [recent.length, countDistinct(recent, 'feedId'), countDistinct(recent, 'feederId'), recent.length];
    return values.map((value, index) => ({ label: labels[index], value: formatNumber(value) }));
  }
  if (active === 'pending') {
    const future = pending.filter((event) => Date.parse(str(event.nextRunAt)) > Date.now()).length;
    const values = [pending.length, future, countDistinct(pending, 'feederId'), failures.length];
    return values.map((value, index) => ({ label: labels[index], value: formatNumber(value), tone: index === 3 && value ? 'failed' : undefined }));
  }
  if (active === 'failures') {
    const values = [failures.length, countDistinct(failures, 'feedId'), countDistinct(failures, 'feederId'), failures[0] ? relativeTime(failures[0].happenedAt) : '—'];
    return values.map((value, index) => ({ label: labels[index], value: typeof value === 'number' ? formatNumber(value) : value, tone: index === 0 && num(value) ? 'failed' : undefined }));
  }
  if (active === 'gaps') {
    const groups = ['cost', 'health', 'traffic', 'finance'];
    return groups.map((group, index) => ({
      label: labels[index],
      value: formatNumber(gaps.filter((gap) => `${str(gap.id)} ${str(gap.label)} ${str(gap.path)}`.toLowerCase().includes(group)).length),
      tone: 'missing',
    }));
  }
  if (active === 'overview') {
    const unhealthy = feeds.filter((feed) => str(feed.health) !== 'smooth').length;
    return [
      { label: labels[0], value: formatNumber(unhealthy), tone: unhealthy ? 'failed' : 'live' },
      { label: labels[1], value: relativeTime(payload.generatedAt) },
      { label: labels[2], value: formatNumber(recent.length) },
      { label: labels[3], value: 'READ ONLY', tone: 'readonly' },
    ];
  }
  return fallback();
}

function PipelineRail({
  active,
  payload,
}: {
  active: DomainId;
  payload: CommandPayload;
}) {
  const metrics = pipelineMetrics(payload, active);
  const title = active === 'overview'
    ? 'SYSTEM MATRIX'
    : active === 'feeds'
      ? 'FEED HEALTH SNAPSHOT'
      : `${allNavItems.find((item) => item.id === active)?.label.toUpperCase()} RAIL`;

  return (
    <section className="cmd-panel cmd-pipeline">
      <div className="cmd-panel-head">
        <span>{title}</span>
        <strong>{active === 'feeds' ? 'live operating totals' : 'source totals'}</strong>
      </div>
      <div className="cmd-pipeline-track">
        {metrics.map((metric) => (
          <div key={metric.label} className={metric.tone ? `is-${metric.tone}` : ''}>
            <span>{metric.label}</span>
            <strong>{metric.value}</strong>
          </div>
        ))}
      </div>
    </section>
  );
}

function CommandTable({
  rows,
  selected,
  onSelect,
}: {
  rows: CommandRow[];
  selected: CommandRow | null;
  onSelect: (row: CommandRow) => void;
}) {
  return (
    <section className="cmd-panel cmd-table-panel">
      <div className="cmd-panel-head">
        <span>RECORDS IN THIS VIEW</span>
        <strong>{rows.length} visible</strong>
      </div>
      {rows.length === 0 ? (
        <div className="cmd-empty">
          <Boxes size={24} />
          <strong>No records</strong>
          <span>This scope has no rows for the selected view.</span>
        </div>
      ) : (
        <div className="cmd-record-list">
          {rows.map((row) => (
            <button
              key={`${row.source}:${row.id}`}
              type="button"
              className={selected?.id === row.id && selected.source === row.source ? 'cmd-record-card is-selected' : 'cmd-record-card'}
              onClick={() => onSelect(row)}
            >
              <span className="cmd-record-state">
                <StatusChip status={row.status} />
                <small>{relativeTime(row.updatedAt)}</small>
              </span>
              {row.source === 'checkpoint_jobs' || row.source === 'post_metrics' ? (
                <PostRecordCell row={row} />
              ) : (
                <span className="cmd-record-copy">
                  <strong>{row.title}</strong>
                  <small>{row.subtitle}</small>
                </span>
              )}
              <span className="cmd-record-meta">
                <em>{row.source}</em>
                <strong>{row.metric || row.eyebrow}</strong>
              </span>
            </button>
          ))}
        </div>
      )}
    </section>
  );
}

function GapChecklist({ gaps }: { gaps: Dict[] }) {
  const groups = ['cost', 'health', 'traffic', 'finance', 'platform'];
  return (
    <section className="cmd-panel">
      <div className="cmd-panel-head">
        <span>GAP CHECKLIST</span>
        <strong>{gaps.length}</strong>
      </div>
      <div className="cmd-gap-grid">
        {groups.map((group) => {
          const groupGaps = gaps.filter((gap) => `${gap.id} ${gap.label} ${gap.path}`.toLowerCase().includes(group));
          return (
            <div key={group}>
              <span>{group}</span>
              <strong>{groupGaps.length || '-'}</strong>
            </div>
          );
        })}
      </div>
    </section>
  );
}

function InspectorRail({
  payload,
  selected,
  active,
  reduced,
  motionProps,
}: {
  payload: CommandPayload | null;
  selected: CommandRow | null;
  active: DomainId;
  reduced: boolean;
  motionProps: CommandRegionMotionProps;
}) {
  const [copiedRecordKey, setCopiedRecordKey] = useState<string | null>(null);
  const [expandedRecordKey, setExpandedRecordKey] = useState<string | null>(null);
  const access = dict(payload?.access);
  const finance = dict(payload?.finance);
  const assumptions = dict(finance.assumptions);
  const realSurfaces = dict(finance.realSurfaces);
  const preview = selected?.raw || {};
  const previewEntries = Object.entries(preview)
    .filter(([, value]) => value == null || ['string', 'number', 'boolean'].includes(typeof value))
    .slice(0, 12);
  const sourceHref = active === 'fire'
    ? '/fire'
    : ['checkpoints', 'media', 'intelligence'].includes(active)
      ? '/read'
      : ['account', 'feeds'].includes(active)
        ? '/'
        : active === 'notifications'
          ? '/profile'
          : null;

  const selectedRecordKey = selected ? `${selected.source}:${selected.id}` : null;
  const copied = Boolean(selectedRecordKey && copiedRecordKey === selectedRecordKey);
  const showRaw = Boolean(selectedRecordKey && expandedRecordKey === selectedRecordKey);

  const copyRecord = useCallback(async () => {
    if (!selected) return;
    try {
      await navigator.clipboard.writeText(JSON.stringify(selected.raw, null, 2));
      setCopiedRecordKey(`${selected.source}:${selected.id}`);
    } catch {
      setCopiedRecordKey(null);
    }
  }, [selected]);

  return (
    <motion.aside
      className="cmd-inspector"
      data-command-motion="inspector"
      {...motionProps}
    >
      <section className="cmd-panel cmd-detail-card">
        <div className="cmd-panel-head">
          <span>SELECTED RECORD</span>
          {selected ? <StatusChip status={selected.status} /> : <StatusChip status="info" />}
        </div>
        <div className="cmd-inspector-body-stack">
          <AnimatePresence initial={false} mode="sync">
            {selected ? (
              <motion.div
                key={selectedRecordKey}
                className="cmd-detail-body"
                initial={reduced ? { opacity: 0.96 } : {
                  opacity: 0.98,
                  transform: 'translate3d(0px, 4px, 0px) scale(0.997)',
                }}
                animate={{ opacity: 1, transform: commandRestingTransform }}
                exit={reduced ? { opacity: 0.06 } : {
                  opacity: 0,
                  transform: 'translate3d(0px, -2px, 0px) scale(0.998)',
                  transition: {
                    opacity: { duration: 0.1, ease: commandEaseOut },
                    transform: { duration: 0.12, ease: commandEaseOut },
                  },
                }}
                transition={reduced ? { duration: 0.1 } : {
                  opacity: { duration: 0.16, ease: commandEaseOut },
                  transform: { duration: 0.2, ease: commandEaseOut },
                }}
              >
              <h2>{selected.title}</h2>
              <p>{selected.subtitle}</p>
              <div className="cmd-inspector-actions">
                <button type="button" onClick={copyRecord}>
                  {copied ? <Check size={13} aria-hidden="true" /> : <Copy size={13} aria-hidden="true" />}
                  {copied ? 'Copied record' : 'Copy record'}
                </button>
                {sourceHref ? (
                  <a href={sourceHref}>
                    <ExternalLink size={13} aria-hidden="true" />
                    Open source surface
                  </a>
                ) : null}
              </div>
              <div className="cmd-inspector-kv">
                <span>View</span><strong>{labelize(active)}</strong>
                <span>Source</span><strong>{selected.source}</strong>
                <span>Updated</span><strong>{shortDate(selected.updatedAt)}</strong>
                <span>Metric</span><strong>{selected.metric || selected.eyebrow}</strong>
                {previewEntries.map(([key, value]) => (
                  <div key={key} className="cmd-kv-pair">
                    <span>{labelize(key)}</span>
                    <strong>{value == null ? 'null' : String(value)}</strong>
                  </div>
                ))}
              </div>
              <div className="cmd-raw-record">
                <button
                  type="button"
                  className="cmd-raw-record-trigger"
                  onClick={() => setExpandedRecordKey((current) => current === selectedRecordKey ? null : selectedRecordKey)}
                  aria-expanded={showRaw}
                >
                  <span>Full raw record</span>
                  <ChevronRight className={showRaw ? 'is-open' : ''} size={14} aria-hidden="true" />
                </button>
                <AnimatePresence initial={false}>
                  {showRaw ? (
                    <motion.pre
                      key="raw-record"
                      initial={reduced ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0px, -4px, 0px) scale(0.997)' }}
                      animate={{ opacity: 1, transform: commandRestingTransform }}
                      exit={reduced ? { opacity: 0 } : {
                        opacity: 0,
                        transform: 'translate3d(0px, -2px, 0px) scale(0.998)',
                        transition: {
                          opacity: { duration: 0.1, ease: commandEaseOut },
                          transform: { duration: 0.12, ease: commandEaseOut },
                        },
                      }}
                      transition={reduced ? { duration: 0.1 } : { duration: 0.2, ease: commandEaseOut }}
                    >
                      {JSON.stringify(preview, null, 2)}
                    </motion.pre>
                  ) : null}
                </AnimatePresence>
              </div>
              <span className="cmd-sr-only" aria-live="polite">{copied ? 'Record copied to clipboard.' : ''}</span>
              </motion.div>
            ) : (
              <motion.div
                key="empty"
                className="cmd-empty small"
                initial={reduced
                  ? { opacity: 0.96 }
                  : { opacity: 0.98, transform: 'translate3d(0px, 4px, 0px) scale(0.997)' }}
                animate={{ opacity: 1, transform: commandRestingTransform }}
                exit={reduced
                  ? { opacity: 0 }
                  : { opacity: 0, transform: 'translate3d(0px, -2px, 0px) scale(0.998)' }}
                transition={{ duration: reduced ? 0.1 : 0.16, ease: commandEaseOut }}
              >
                <Boxes size={20} />
                <strong>Select a row</strong>
                <span>Record context lands here.</span>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </section>

      <section className="cmd-panel cmd-readonly-note">
        <div className="cmd-panel-head">
          <span>ACCESS</span>
          <StatusChip status="readonly" />
        </div>
        <p>{str(access.note, 'Read-only command scope. No mutation controls are exposed.')}</p>
        <div className="cmd-inspector-kv">
          <span>Email</span><strong>{str(access.signedInEmail, 'Admin')}</strong>
          <span>Mode</span><strong>{labelize(access.mode, 'Scoped')}</strong>
        </div>
      </section>

      <section className="cmd-panel">
        <div className="cmd-panel-head">
          <span>FINANCE TRUTH</span>
          <StatusChip status="partial" />
        </div>
        <div className="cmd-inspector-kv">
          <span>Price / feeder</span><strong>{formatInr(assumptions.plannedPriceInrPerFeeder)}</strong>
          <span>Razorpay</span><strong>not live</strong>
          <span>Known paid</span><strong>{formatInr(realSurfaces.paidTransactionsInr)}</strong>
          <span>Known pending</span><strong>{formatInr(realSurfaces.pendingTransactionsInr)}</strong>
        </div>
      </section>
    </motion.aside>
  );
}

export default function CommandHubClient() {
  const [payload, setPayload] = useState<CommandPayload | null>(null);
  const [error, setError] = useState<LoadFailure | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastLoadedAt, setLastLoadedAt] = useState<string | null>(null);
  const [snapshotClock, setSnapshotClock] = useState(() => Date.now());
  const [active, setActive] = useState<DomainId>('pending');
  const [query, setQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [selected, setSelected] = useState<CommandRow | null>(null);
  const [runwayExpanded, setRunwayExpanded] = useState(false);
  const reduceCommandMotion = Boolean(useReducedMotion());
  const compactCommandMotion = useSyncExternalStore(
    subscribeCommandMotionViewport,
    commandMotionViewportSnapshot,
    commandMotionViewportServerSnapshot,
  );
  const {
    appShellStyle,
    useBrowserPageScroll,
  } = useMobileImmersiveViewport('(min-width: 881px)');
  const requestRef = useRef(0);
  const abortRef = useRef<AbortController | null>(null);
  const inFlightRef = useRef(false);
  const hasPayloadRef = useRef(false);
  const mainRef = useRef<HTMLElement | null>(null);
  const pageScrollModeRef = useRef(compactCommandMotion);
  const activeRef = useRef<DomainId>('pending');
  const scrollPositionsRef = useRef(new Map<DomainId, number>());
  const queryRef = useRef('');
  const statusFilterRef = useRef('all');
  const queryByDomainRef = useRef(new Map<DomainId, string>());
  const statusByDomainRef = useRef(new Map<DomainId, string>());
  const navigationMotionRef = useRef<NavigationMotion>('fluid');
  const navigationDirectionRef = useRef<NavigationDirection>('neutral');

  const loadCommand = useCallback(async () => {
    if (inFlightRef.current) return;
    const requestId = ++requestRef.current;
    const controller = new AbortController();
    abortRef.current = controller;
    inFlightRef.current = true;
    setError(null);
    if (hasPayloadRef.current) setRefreshing(true);
    else setLoading(true);

    try {
      const response = await fetch('/api/command', {
        headers: { Accept: 'application/json' },
        cache: 'no-store',
        signal: controller.signal,
      });
      const body = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw {
          status: response.status,
          message: str((body as Dict).error, `Command hub failed with ${response.status}`),
        };
      }
      if (requestRef.current !== requestId) return;
      hasPayloadRef.current = true;
      setPayload(body as CommandPayload);
      setLastLoadedAt(new Date().toISOString());
      setSnapshotClock(Date.now());
    } catch (loadError) {
      if (controller.signal.aborted || requestRef.current !== requestId) return;
      const failure = dict(loadError);
      setError({
        status: num(failure.status) || 500,
        message: loadError instanceof Error
          ? loadError.message
          : str(failure.message, 'Failed to load command hub'),
      });
    } finally {
      if (requestRef.current === requestId) {
        inFlightRef.current = false;
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, []);

  const updateQuery = useCallback((value: string) => {
    queryRef.current = value;
    setQuery(value);
  }, []);

  const updateStatusFilter = useCallback((value: string) => {
    statusFilterRef.current = value;
    setStatusFilter(value);
  }, []);

  const navigateDomain = useCallback((
    domain: DomainId,
    historyMode: 'push' | 'none' = 'push',
    motion: NavigationMotion = 'fluid',
  ) => {
    const previousDomain = activeRef.current;
    if (domain === previousDomain) return;
    navigationMotionRef.current = motion;
    const previousPosition = navigationPosition(previousDomain);
    const nextPosition = navigationPosition(domain);
    navigationDirectionRef.current = nextPosition === previousPosition
      ? 'neutral'
      : nextPosition > previousPosition
        ? 'forward'
        : 'backward';
    const currentScrollTop = compactCommandMotion
      ? window.scrollY
      : mainRef.current?.scrollTop ?? 0;
    scrollPositionsRef.current.set(previousDomain, currentScrollTop);
    queryByDomainRef.current.set(previousDomain, queryRef.current);
    statusByDomainRef.current.set(previousDomain, statusFilterRef.current);
    const nextQuery = queryByDomainRef.current.get(domain) || '';
    const nextStatus = statusByDomainRef.current.get(domain) || 'all';

    const commitNavigation = () => {
      activeRef.current = domain;
      queryRef.current = nextQuery;
      statusFilterRef.current = nextStatus;
      setActive(domain);
      setQuery(nextQuery);
      setStatusFilter(nextStatus);
      if (historyMode === 'push') {
        const url = new URL(window.location.href);
        if (domain === 'pending') url.searchParams.delete('view');
        else url.searchParams.set('view', domain);
        window.history.pushState({ commandView: domain }, '', url);
      }
    };

    commitNavigation();
  }, [compactCommandMotion]);

  useLayoutEffect(() => {
    const previousPageScrollMode = pageScrollModeRef.current;
    const scrollModeChanged = previousPageScrollMode !== compactCommandMotion;
    const nextScrollTop = scrollModeChanged
      ? previousPageScrollMode
        ? window.scrollY
        : mainRef.current?.scrollTop ?? 0
      : scrollPositionsRef.current.get(active) || 0;
    if (scrollModeChanged) {
      scrollPositionsRef.current.set(active, nextScrollTop);
      pageScrollModeRef.current = compactCommandMotion;
    }
    if (compactCommandMotion) {
      window.scrollTo(0, nextScrollTop);
      return;
    }
    if (mainRef.current) mainRef.current.scrollTop = nextScrollTop;
  }, [active, compactCommandMotion]);

  useEffect(() => {
    const syncFromUrl = () => {
      const requested = new URL(window.location.href).searchParams.get('view');
      navigateDomain(isDomainId(requested) ? requested : 'pending', 'none', 'instant');
    };
    syncFromUrl();
    window.addEventListener('popstate', syncFromUrl);
    return () => window.removeEventListener('popstate', syncFromUrl);
  }, [navigateDomain]);

  useEffect(() => {
    void loadCommand();
    return () => {
      requestRef.current += 1;
      abortRef.current?.abort();
      abortRef.current = null;
      inFlightRef.current = false;
    };
  }, [loadCommand]);

  const systems = useMemo(() => systemHealth(payload), [payload]);
  const runtimeSystems = useMemo(() => runtimeHealth(payload), [payload]);
  const attention = useMemo(() => {
    const readiness = dict(payload?.operationalReadiness);
    const readinessDomain = (source: string): DomainId => {
      const key = source.toLowerCase();
      if (key.includes('worker') || key.includes('run_job')) return 'engine';
      if (key.includes('schedule') || key.includes('checkpoint')) return 'checkpoints';
      if (key.includes('signal') || key.includes('fire')) return 'fire';
      if (key.includes('media')) return 'media';
      if (key.includes('notification') || key.includes('push')) return 'notifications';
      if (key.includes('feedops') || key.includes('queue')) return 'pending';
      if (key.includes('feed')) return 'feeds';
      return 'gaps';
    };
    const readinessAttention: Attention[] = [
      ...list(readiness.blockers).map((item) => ({
        id: `readiness:blocker:${str(item.id, str(item.source))}`,
        label: str(item.source, 'Readiness blocker'),
        source: str(item.source, 'operationalReadiness'),
        detail: str(item.detail, 'Production readiness is blocked.'),
        status: 'failed' as const,
        updatedAt: str(payload?.generatedAt) || null,
        domain: readinessDomain(str(item.source)),
      })),
      ...list(readiness.warnings).map((item) => ({
        id: `readiness:warning:${str(item.id, str(item.source))}`,
        label: str(item.source, 'Readiness warning'),
        source: str(item.source, 'operationalReadiness'),
        detail: str(item.detail, 'Production readiness needs review.'),
        status: 'partial' as const,
        updatedAt: str(payload?.generatedAt) || null,
        domain: readinessDomain(str(item.source)),
      })),
    ];
    const runtimeAttention: Attention[] = runtimeSystems
      .filter((surface) => surface.status !== 'live')
      .map((surface) => ({
        id: `runtime:${surface.id}`,
        label: surface.label,
        source: 'runtime',
        detail: surface.note,
        status: surface.status,
        updatedAt: surface.updatedAt,
        domain: surface.id === 'workers' ? 'engine' : 'checkpoints',
      }));
    return [...readinessAttention, ...runtimeAttention, ...attentionQueue(payload, systems)]
      .sort((a, b) => statusOrder[a.status] - statusOrder[b.status]);
  }, [payload, runtimeSystems, systems]);
  const allRows = useMemo(() => rowsForDomain(payload, active), [payload, active]);
  const deferredQuery = useDeferredValue(query.trim().toLowerCase());
  const feedOps = useMemo(() => dict(payload?.feedOps), [payload]);
  const opsFeeds = useMemo(() => list(feedOps.feeds), [feedOps]);
  const timelineEvents = useMemo(() => list(dict(payload?.timeline).events).map(normalizeTimelineEvent), [payload]);
  const opsRecent = useMemo(() => {
    const combined = [...timelineEvents, ...list(feedOps.recentActivity)];
    const unique = new Map(combined.map((event) => [str(event.id), event]));
    return Array.from(unique.values())
      .sort((a, b) => Date.parse(str(b.happenedAt)) - Date.parse(str(a.happenedAt)))
      .slice(0, 120);
  }, [feedOps, timelineEvents]);
  const scheduledAhead = useMemo(() => list(feedOps.scheduledAhead), [feedOps]);
  const opsPending = useMemo(() => scheduledAhead.length ? scheduledAhead : list(feedOps.pendingAhead), [feedOps, scheduledAhead]);
  const opsNow = useMemo(() => [...list(feedOps.overdueWork), ...list(feedOps.inProgress)], [feedOps]);
  const opsFailures = useMemo(() => list(feedOps.failures), [feedOps]);
  const runwaySource = useMemo(() => {
    const currentState = list(feedOps.currentState);
    if (currentState.length) return currentState;
    const fallback = [
      ...list(feedOps.overdueWork),
      ...list(feedOps.inProgress),
      ...list(feedOps.pendingAhead),
      ...list(feedOps.scheduledAhead),
    ];
    return Array.from(new Map(fallback.map((event, index) => [str(event.id, `event:${index}`), event])).values());
  }, [feedOps]);
  const runway = useMemo(
    () => partitionCommandRunway(runwaySource as CommandEvent[], snapshotClock),
    [runwaySource, snapshotClock],
  );
  const visibleRunway = useMemo(() => ({
    overdue: runway.overdue.filter((event) => queryMatches(event, deferredQuery)),
    running: runway.running.filter((event) => queryMatches(event, deferredQuery)),
    dueNext: runway.dueNext.filter((event) => queryMatches(event, deferredQuery)),
    later: runway.later.filter((event) => queryMatches(event, deferredQuery)),
  }), [deferredQuery, runway]);
  const statuses = useMemo(() => {
    if (active === 'feeds') return ['all', 'smooth', 'pending', 'stale', 'failed', 'missing'];
    const set = new Set(allRows.map((row) => row.status).filter(Boolean));
    return ['all', ...Array.from(set).sort()];
  }, [active, allRows]);
  const rows = useMemo(() => {
    return allRows.filter((row) => {
      const matchesStatus = statusFilter === 'all' || row.status === statusFilter;
      if (!matchesStatus) return false;
      if (!deferredQuery) return true;
      return [row.title, row.subtitle, row.status, row.source, row.eyebrow, row.metric]
        .join(' ')
        .toLowerCase()
        .includes(deferredQuery);
    });
  }, [allRows, deferredQuery, statusFilter]);
  const visibleFeeds = useMemo(() => {
    return opsFeeds.filter((feed) => {
      const matchesStatus = statusFilter === 'all' || str(feed.health) === statusFilter;
      if (!matchesStatus) return false;
      return queryMatches(feed, deferredQuery);
    });
  }, [deferredQuery, opsFeeds, statusFilter]);
  const visibleRecent = useMemo(() => opsRecent.filter((event) => queryMatches(event, deferredQuery)), [deferredQuery, opsRecent]);
  const visiblePending = useMemo(() => opsPending.filter((event) => queryMatches(event, deferredQuery)), [deferredQuery, opsPending]);
  const visibleNow = useMemo(() => opsNow.filter((event) => queryMatches(event, deferredQuery)), [deferredQuery, opsNow]);
  const visibleFailures = useMemo(() => opsFailures.filter((event) => queryMatches(event, deferredQuery)), [deferredQuery, opsFailures]);
  const derivedCriticalCount = systems.filter((system) => system.status === 'failed').length
    + runtimeSystems.filter((system) => system.status === 'failed').length
    + list(payload?.instrumentationGaps).filter((gap) => statusKind(gap.status) === 'failed').length;
  const readiness = dict(payload?.operationalReadiness);
  const readinessStatus = str(readiness.status).toLowerCase();
  const hasReadinessContract = ['ready', 'watch', 'blocked'].includes(readinessStatus);
  const criticalCount = hasReadinessContract ? list(readiness.blockers).length : derivedCriticalCount;
  const betaState: 'READY' | 'WATCH' | 'BLOCKED' = hasReadinessContract
    ? readinessStatus.toUpperCase() as 'READY' | 'WATCH' | 'BLOCKED'
    : criticalCount
      ? 'BLOCKED'
      : attention.length
        ? 'WATCH'
        : 'READY';
  const isAuthError = error?.status === 401;
  const selectableRows = useMemo(() => {
    if (active === 'activity') return visibleRecent.map((event) => rowFromOpsEvent(event));
    if (active === 'pending') return [
      ...visibleRunway.overdue,
      ...visibleRunway.running,
      ...visibleRunway.dueNext,
      ...visibleRunway.later,
    ].map((event) => rowFromOpsEvent(event, 'upcoming'));
    if (active === 'failures') return visibleFailures.map((event) => rowFromOpsEvent(event));
    if (active === 'overview') return [
      ...visibleRecent.map((event) => rowFromOpsEvent(event)),
      ...visiblePending.map((event) => rowFromOpsEvent(event, 'upcoming')),
      ...visibleNow.map((event) => rowFromOpsEvent(event, 'upcoming')),
      ...visibleFailures.map((event) => rowFromOpsEvent(event)),
    ];
    return rows;
  }, [active, rows, visibleFailures, visibleNow, visiblePending, visibleRecent, visibleRunway]);

  const resolvedSelected = useMemo(() => {
    if (selected && selectableRows.some((row) => row.id === selected.id && row.source === selected.source)) return selected;
    return selectableRows[0] || null;
  }, [selectableRows, selected]);

  const selectedRunwayGroup = useMemo(() => {
    if (!resolvedSelected || active !== 'pending') return [];
    return runway.allOpen.filter((event) => runwayGroupKey(event) === runwayGroupKey(resolvedSelected.raw));
  }, [active, resolvedSelected, runway.allOpen]);

  useEffect(() => {
    setSelected((current) => {
      if (current && selectableRows.some((row) => row.id === current.id && row.source === current.source)) return current;
      return selectableRows[0] || null;
    });
  }, [active, selectableRows]);

  const exploreActive = !primaryTaskIds.has(active);
  const primaryActiveIndex = primaryTaskNav.findIndex((item) => item.id === (exploreActive ? 'feeds' : active));
  const commandMotionDirection = navigationDirectionRef.current;
  const commandMotionMode = navigationMotionRef.current;
  const commandMotionState = useMemo<CommandMotionState>(() => ({
    direction: commandMotionDirection,
    mode: commandMotionMode,
    compact: compactCommandMotion,
    reduced: reduceCommandMotion,
  }), [commandMotionDirection, commandMotionMode, compactCommandMotion, reduceCommandMotion]);
  const commandRegionMotions = useMemo(() => ({
    summary: commandRegionMotion('summary', commandMotionState),
    controls: commandRegionMotion('controls', commandMotionState),
    body: commandRegionMotion('body', commandMotionState),
    inspector: commandRegionMotion('inspector', commandMotionState),
  }), [commandMotionState]);
  const commandIndicatorTransition = commandMotionState.mode === 'instant' || commandMotionState.reduced
    ? { duration: 0 }
    : commandIndicatorSpring;
  const mobileIndicatorIndex = Math.max(0, primaryActiveIndex);
  const mobileIndicatorTransform = `translate3d(calc(${mobileIndicatorIndex * 100}% + ${mobileIndicatorIndex * 4}px), 0px, 0px)`;
  const revision = Date.parse(str(payload?.generatedAt)) || runway.allOpen.length;

  return (
    <div
      className={`cmd-stage ${active === 'pending' ? 'is-runway' : ''}`}
      data-command-shell
      data-browser-page-scroll={useBrowserPageScroll ? 'true' : undefined}
      aria-busy={loading || refreshing}
      style={appShellStyle}
    >
      <div className="cmd-scan" aria-hidden="true" />
      <div className="cmd-top-chrome">
        <PortalHeader payload={payload} betaState={betaState} criticalCount={criticalCount} />
        <CommandSyncBar
          refreshing={refreshing}
          lastLoadedAt={lastLoadedAt}
          refreshError={payload ? error : null}
          onRefresh={() => void loadCommand()}
        />
      </div>
      <div className={`cmd-refresh-line ${refreshing ? 'is-refreshing' : ''}`} aria-hidden="true" />

      <div className="cmd-shell">
        <aside className="cmd-rail">
          <nav
            className="cmd-task-nav"
            aria-label="Command timeline"
            data-nav-motion={navigationMotionRef.current}
            data-nav-direction={navigationDirectionRef.current}
          >
            <motion.span
              className="cmd-nav-indicator"
              aria-hidden="true"
              initial={false}
              animate={{ transform: `translate3d(0px, ${Math.max(0, primaryActiveIndex) * 108}px, 0px)` }}
              transition={commandIndicatorTransition}
            />
            {primaryTaskNav.map((item) => {
              const Icon = item.icon;
              const isActive = item.id === 'feeds' ? exploreActive : active === item.id;
              return (
                <button
                  key={item.id}
                  type="button"
                  className={isActive ? 'is-active' : ''}
                  onClick={(event) => navigateDomain(item.id, 'push', event.detail === 0 ? 'instant' : 'fluid')}
                  aria-current={isActive ? 'page' : undefined}
                >
                  <Icon size={25} strokeWidth={1.75} aria-hidden="true" />
                  <em>{item.label}</em>
                </button>
              );
            })}
          </nav>
          <span className="cmd-rail-mode">RO</span>
        </aside>

        <main ref={mainRef} className="cmd-main" aria-label="Command Hub workspace">
          {loading && !payload ? (
            <div className="cmd-loading-grid" role="status" aria-label="Loading Command Hub">
              {Array.from({ length: 10 }).map((_, index) => <span key={index} />)}
            </div>
          ) : error && !payload ? (
            <section className={`cmd-panel cmd-error ${isAuthError ? 'cmd-auth-error' : ''}`} role="alert">
              {isAuthError ? <ShieldCheck size={28} /> : <AlertTriangle size={28} />}
              <strong>{isAuthError ? 'Sign in required' : 'Command hub unavailable'}</strong>
              <span>
                {isAuthError
                  ? 'The command hub is read-only, but it still needs your FeedMe session before it can load account data.'
                  : error.message}
              </span>
              {isAuthError ? (
                <a className="cmd-primary-link" href="/login?next=%2Fcommand">Sign in to Command Hub</a>
              ) : (
                <button type="button" className="cmd-primary-link" onClick={() => void loadCommand()}>Retry command read</button>
              )}
            </section>
          ) : payload ? (
            <>
              <div
                className="cmd-mobile-tabs"
                role="tablist"
                aria-label="Command timeline"
                data-active-index={Math.max(0, primaryActiveIndex)}
                data-nav-motion={navigationMotionRef.current}
                data-nav-direction={navigationDirectionRef.current}
              >
                <motion.span
                  className="cmd-mobile-tab-indicator"
                  aria-hidden="true"
                  initial={false}
                  animate={{ transform: mobileIndicatorTransform }}
                  transition={commandIndicatorTransition}
                />
                {primaryTaskNav.map((item) => {
                  const isActive = item.id === 'feeds' ? exploreActive : active === item.id;
                  return (
                    <button
                      key={item.id}
                      type="button"
                      role="tab"
                      className={isActive ? 'is-active' : ''}
                      onClick={(event) => navigateDomain(item.id, 'push', event.detail === 0 ? 'instant' : 'fluid')}
                      aria-selected={isActive}
                    >
                      {item.label}
                    </button>
                  );
                })}
              </div>
              <span className="cmd-sr-only" aria-live="polite">Showing {primaryTaskNav.find((item) => item.id === active)?.label || 'Explore'}</span>

              <div
                className={`cmd-content ${active === 'pending' ? 'is-runway' : ''}`}
                data-command-view={active}
                data-nav-motion={navigationMotionRef.current}
                data-nav-direction={navigationDirectionRef.current}
              >
                <section className="cmd-center">
                  <div className="cmd-view-surface">
                    <div className="cmd-motion-slot is-summary">
                      <AnimatePresence initial={false} mode="sync">
                        <motion.div
                          key={exploreActive ? 'explore-summary' : `summary:${active}`}
                          className="cmd-summary-frame"
                          data-command-motion="summary"
                          {...commandRegionMotions.summary}
                        >
                          <div className="cmd-summary-content">
                            {active === 'pending' ? (
                              <RunwayMetrics
                                total={runway.allOpen.length}
                                overdue={runway.overdue.length}
                                running={runway.running.length}
                                dueSoon={runway.dueWithinTwelveHours.length}
                                revision={revision}
                                sampled={runwaySource.length >= 240}
                              />
                            ) : (
                              <BetaReadinessBand betaState={betaState} criticalCount={criticalCount} attention={attention} />
                            )}
                          </div>
                        </motion.div>
                      </AnimatePresence>
                    </div>

                    <div className="cmd-motion-slot is-controls">
                      <AnimatePresence initial={false} mode="sync">
                        <CommandViewToolbar
                          key={exploreActive ? 'explore-controls' : `controls:${active}`}
                          active={active}
                          exploreActive={exploreActive}
                          query={query}
                          statusFilter={statusFilter}
                          statuses={statuses}
                          total={runway.allOpen.length}
                          sampled={runwaySource.length >= 240}
                          expanded={runwayExpanded}
                          onQuery={updateQuery}
                          onStatusFilter={updateStatusFilter}
                          onToggleExpanded={() => setRunwayExpanded((current) => !current)}
                          onNavigate={(domain, motion = 'fluid') => navigateDomain(domain, 'push', motion)}
                          motionProps={commandRegionMotions.controls}
                        />
                      </AnimatePresence>
                    </div>

                    <div className="cmd-motion-slot is-body">
                      <AnimatePresence initial={false} mode="sync">
                        <motion.div
                          key={`body:${active}`}
                          className="cmd-view-body"
                          data-command-motion="body"
                          {...commandRegionMotions.body}
                        >
                          {active === 'overview' ? (
                            <>
                              <SystemHealthMatrix
                                systems={systems}
                                runtime={runtimeSystems}
                                active={active}
                                onNavigate={(domain, motion = 'fluid') => navigateDomain(domain, 'push', motion)}
                              />
                              <ProductSurfaceRegistry surfaces={list(payload.productSurfaces)} />
                              <AttentionQueue
                                attention={attention}
                                onNavigate={(domain, motion = 'fluid') => navigateDomain(domain, 'push', motion)}
                                onSelect={setSelected}
                              />
                              <FeedOpsBoard feeds={visibleFeeds.slice(0, 8)} onSelect={setSelected} />
                              {visibleNow.length ? (
                                <OpsEventList title="Operational Now / Overdue" events={visibleNow} empty="No current operational work" onSelect={setSelected} time="upcoming" />
                              ) : null}
                              <div className="cmd-overview-grid">
                                <OpsEventList title="Recent App Activity" events={visibleRecent.slice(0, 6)} empty="No recent activity" onSelect={setSelected} />
                                <OpsEventList title="Pending Ahead" events={visiblePending.slice(0, 6)} empty="Nothing queued" onSelect={setSelected} time="upcoming" />
                              </div>
                              <OpsEventList title="Failure Queue" events={visibleFailures.slice(0, 6)} empty="No failures" onSelect={setSelected} />
                            </>
                          ) : active === 'feeds' ? (
                            <>
                              <PipelineRail active={active} payload={payload} />
                              <FeedOpsBoard feeds={visibleFeeds} onSelect={setSelected} />
                            </>
                          ) : active === 'activity' ? (
                            <>
                              <PipelineRail active={active} payload={payload} />
                              <OpsEventList title="Recent App Activity" events={visibleRecent} empty="No matching recent activity" onSelect={setSelected} />
                            </>
                          ) : active === 'pending' ? (
                            <TemporalRunway
                              overdue={visibleRunway.overdue}
                              running={visibleRunway.running}
                              dueNext={visibleRunway.dueNext}
                              later={visibleRunway.later}
                              selected={resolvedSelected}
                              expanded={runwayExpanded}
                              reduced={reduceCommandMotion}
                              onSelect={setSelected}
                            />
                          ) : active === 'failures' ? (
                            <>
                              <PipelineRail active={active} payload={payload} />
                              <OpsEventList title="Failure Queue" events={visibleFailures} empty="No matching failures" onSelect={setSelected} />
                            </>
                          ) : active === 'gaps' ? (
                            <>
                              <PipelineRail active={active} payload={payload} />
                              <GapChecklist gaps={list(payload.instrumentationGaps)} />
                              <CommandTable rows={rows} selected={resolvedSelected} onSelect={setSelected} />
                            </>
                          ) : (
                            <>
                              {active === 'intelligence' || active === 'finance' ? <NotConnectedNotice domain={active} /> : null}
                              <PipelineRail active={active} payload={payload} />
                              <CommandTable rows={rows} selected={resolvedSelected} onSelect={setSelected} />
                            </>
                          )}
                        </motion.div>
                      </AnimatePresence>
                    </div>
                  </div>
                </section>

                <div className="cmd-inspector-slot">
                  <AnimatePresence initial={false} mode="sync">
                    {active === 'pending' ? (
                      <RunwayInspector
                        key="inspector:pending"
                        payload={payload}
                        selected={resolvedSelected}
                        related={selectedRunwayGroup}
                        reduced={reduceCommandMotion}
                        motionProps={commandRegionMotions.inspector}
                      />
                    ) : (
                      <InspectorRail
                        key={active}
                        payload={payload}
                        selected={resolvedSelected}
                        active={active}
                        reduced={reduceCommandMotion}
                        motionProps={commandRegionMotions.inspector}
                      />
                    )}
                  </AnimatePresence>
                </div>
              </div>
            </>
          ) : null}
        </main>
      </div>
    </div>
  );
}
