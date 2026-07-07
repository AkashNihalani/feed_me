'use client';

/* eslint-disable @next/next/no-img-element -- command thumbnails use dynamic read-only media URLs. */

import { useEffect, useMemo, useState } from 'react';
import {
  AlertTriangle,
  Bell,
  Boxes,
  Brain,
  CircleDollarSign,
  Flame,
  Gauge,
  HardDrive,
  Image as ImageIcon,
  Layers3,
  Lock,
  Network,
  Search,
  ShieldCheck,
  TimerReset,
  Video,
} from 'lucide-react';

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
};

type LoadFailure = {
  status: number;
  message: string;
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
  fire: ['Alerts', 'Signals', 'Hot Posts', 'Suppressed'],
  intelligence: ['Fingerprints', 'Condense', 'D7 Reads', 'Model Calls'],
  media: ['Capture', 'Storage', 'Purge', 'Failed'],
  notifications: ['Subscriptions', 'Pending', 'Sent', 'Failed'],
  finance: ['Planned MRR', 'Paid', 'Pending', 'Cost Gaps'],
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
  const minutes = Math.max(0, Math.round((Date.now() - date.getTime()) / 60000));
  if (minutes < 1) return 'now';
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours}h`;
  return `${Math.round(hours / 24)}d`;
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
    ...rowsForDomain(payload, 'intelligence').slice(0, 5),
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
  const finance = dict(payload.finance);
  const engineTotals = dict(engine.totals);
  const checkpointTotals = dict(checkpoints.totals);
  const fireTotals = dict(fire.totals);
  const intelligenceTotals = dict(intelligence.totals);
  const mediaTotals = dict(media.totals);
  const notificationTotals = dict(notifications.totals);
  const realFinance = dict(finance.realSurfaces);
  const gapCount = list(payload.instrumentationGaps).length;

  return [
    {
      id: 'account',
      label: 'Account',
      status: num(topline.activeFeeders) ? 'live' : 'stale',
      value: formatNumber(topline.activeFeeders),
      note: `${formatNumber(topline.followerReach)} reach`,
      updatedAt: str(account.latestChangeAt) || latestAt(rowsForDomain(payload, 'account')),
    },
    {
      id: 'engine',
      label: 'Engine',
      status: num(engineTotals.failed) ? 'failed' : num(engineTotals.open) ? 'partial' : 'live',
      value: formatNumber(engineTotals.jobs),
      note: `${formatNumber(engineTotals.successPercent)}% success`,
      updatedAt: str(engine.latestChangeAt) || latestAt(rowsForDomain(payload, 'engine')),
    },
    {
      id: 'checkpoints',
      label: 'Checkpoints',
      status: num(checkpointTotals.failed) ? 'failed' : num(checkpointTotals.open) ? 'partial' : 'live',
      value: formatNumber(checkpointTotals.jobs),
      note: `${formatNumber(checkpointTotals.metrics)} metric rows`,
      updatedAt: str(checkpoints.latestChangeAt) || latestAt(rowsForDomain(payload, 'checkpoints')),
    },
    {
      id: 'fire',
      label: 'Fire',
      status: num(fireTotals.erroredSignals) ? 'failed' : num(fireTotals.staleOrSuppressedSignals) ? 'stale' : 'live',
      value: formatNumber(num(fireTotals.alerts) + num(fireTotals.signals)),
      note: `${formatNumber(fireTotals.hotPosts)} hot posts`,
      updatedAt: str(fire.latestChangeAt) || latestAt(rowsForDomain(payload, 'fire')),
    },
    {
      id: 'intelligence',
      label: 'Intelligence',
      status: num(intelligenceTotals.failedModelCalls) ? 'failed' : 'live',
      value: formatNumber(intelligenceTotals.modelCalls),
      note: `${formatNumber(intelligenceTotals.feederFiles)} feeder files`,
      updatedAt: str(intelligence.latestChangeAt) || latestAt(rowsForDomain(payload, 'intelligence')),
    },
    {
      id: 'media',
      label: 'Media',
      status: num(mediaTotals.failed) ? 'failed' : num(mediaTotals.pendingCapture) || num(mediaTotals.purgeQueue) ? 'partial' : 'live',
      value: formatBytes(mediaTotals.knownBytes),
      note: `${formatNumber(mediaTotals.assets)} assets`,
      updatedAt: str(media.latestChangeAt) || latestAt(rowsForDomain(payload, 'media')),
    },
    {
      id: 'notifications',
      label: 'Notifications',
      status: num(notificationTotals.failedJobs) ? 'failed' : num(notificationTotals.openJobs) ? 'partial' : 'live',
      value: formatNumber(notificationTotals.jobs),
      note: `${formatNumber(notificationTotals.activeSubscriptions)} active subs`,
      updatedAt: str(notifications.latestChangeAt) || latestAt(rowsForDomain(payload, 'notifications')),
    },
    {
      id: 'finance',
      label: 'Finance',
      status: 'partial',
      value: formatInr(finance.plannedMonthlyRevenueInr),
      note: `${formatInr(realFinance.paidTransactionsInr)} known paid`,
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

function attentionQueue(payload: CommandPayload | null, systems: SystemHealth[]): Attention[] {
  if (!payload) return [];
  const rows = rowsForDomain(payload, 'overview');
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
    }));

  return [...systemAttention, ...rowAttention]
    .sort((a, b) => statusOrder[a.status] - statusOrder[b.status])
    .slice(0, 10);
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

function rowFromOpsEvent(raw: Dict): CommandRow {
  return makeRow(raw, {
    id: str(raw.id, crypto.randomUUID()),
    title: str(raw.title, 'Ops event'),
    eyebrow: str(raw.source, 'feedOps'),
    subtitle: [str(raw.feedName), str(raw.feederHandle) ? `@${str(raw.feederHandle)}` : '', str(raw.detail)].filter(Boolean).join(' / '),
    status: str(raw.status, 'done'),
    metric: str(raw.kind, str(raw.source, 'event')),
    updatedAt: str(raw.happenedAt) || str(raw.nextRunAt) || null,
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

function OpsEventRow({ event, onSelect }: { event: Dict; onSelect: (row: CommandRow) => void }) {
  const [imageFailed, setImageFailed] = useState(false);
  const thumbnailUrl = str(event.thumbnailUrl);

  return (
    <button type="button" className="cmd-ops-event" onClick={() => onSelect(rowFromOpsEvent(event))}>
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
        <small>{relativeTime(str(event.happenedAt) || str(event.nextRunAt))}</small>
      </span>
    </button>
  );
}

function OpsEventList({
  title,
  events,
  empty,
  onSelect,
}: {
  title: string;
  events: Dict[];
  empty: string;
  onSelect: (row: CommandRow) => void;
}) {
  return (
    <section className="cmd-panel cmd-ops-list">
      <div className="cmd-panel-head">
        <span>{title}</span>
        <strong>{events.length}</strong>
      </div>
      {events.length ? events.map((event) => (
        <OpsEventRow key={str(event.id)} event={event} onSelect={onSelect} />
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

function PortalHeader({
  payload,
  betaState,
  criticalCount,
}: {
  payload: CommandPayload | null;
  betaState: 'READY' | 'WATCH' | 'BLOCKED';
  criticalCount: number;
}) {
  const [now, setNow] = useState(() => new Date());

  useEffect(() => {
    const timer = window.setInterval(() => setNow(new Date()), 1000);
    return () => window.clearInterval(timer);
  }, []);

  return (
    <header className="cmd-portal-header">
      <div>
        <h1>COMMAND CENTER</h1>
        <span>{payload ? `FEEDME / READ-ONLY LIVE OPS / LAST READ ${shortDate(payload.generatedAt, true)}` : 'FEEDME / READ CYCLE PENDING'}</span>
      </div>
      <div className="cmd-header-grid" aria-label="Command state">
        <div><span>IST</span><strong>{shortDate(now.toISOString(), true).split(', ').pop()}</strong></div>
        <div><span>STATE</span><strong className={`is-${betaState.toLowerCase()}`}>{betaState}</strong></div>
        <div><span>CRITICAL</span><strong>{criticalCount}</strong></div>
        <StatusChip status="readonly" />
      </div>
    </header>
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

function PipelineRail({
  active,
  payload,
}: {
  active: DomainId;
  payload: CommandPayload;
}) {
  const labels = pipelineLabels[active];
  const rows = rowsForDomain(payload, active);
  const failed = rows.filter((row) => statusKind(row.status) === 'failed').length;
  const partial = rows.filter((row) => statusKind(row.status) === 'partial').length;
  const live = rows.filter((row) => statusKind(row.status) === 'live').length;
  const missing = active === 'gaps' ? rows.length : 0;
  const values = active === 'finance'
    ? [
      formatInr(dict(payload.finance).plannedMonthlyRevenueInr),
      formatInr(dict(dict(payload.finance).realSurfaces).paidTransactionsInr),
      formatInr(dict(dict(payload.finance).realSurfaces).pendingTransactionsInr),
      String(list(payload.instrumentationGaps).filter((gap) => str(gap.id).includes('cost')).length),
    ]
    : [String(live), String(partial), String(rows.length), String(failed + missing)];

  return (
    <section className="cmd-panel cmd-pipeline">
      <div className="cmd-panel-head">
        <span>{active === 'overview' ? 'SYSTEM MATRIX' : `${allNavItems.find((item) => item.id === active)?.label.toUpperCase()} RAIL`}</span>
        <strong>{rows.length}</strong>
      </div>
      <div className="cmd-pipeline-track">
        {labels.map((label, index) => (
          <div key={label} className={index === labels.length - 1 && failed + missing ? 'is-hot' : ''}>
            <span>{label}</span>
            <strong>{values[index] ?? '0'}</strong>
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
}: {
  payload: CommandPayload | null;
  selected: CommandRow | null;
  active: DomainId;
}) {
  const access = dict(payload?.access);
  const finance = dict(payload?.finance);
  const assumptions = dict(finance.assumptions);
  const realSurfaces = dict(finance.realSurfaces);
  const preview = selected?.raw || {};
  const previewEntries = Object.entries(preview)
    .filter(([, value]) => value == null || ['string', 'number', 'boolean'].includes(typeof value))
    .slice(0, 9);

  return (
    <aside className="cmd-inspector">
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

      <section className="cmd-panel cmd-detail-card">
        <div className="cmd-panel-head">
          <span>INSPECTOR</span>
          {selected ? <StatusChip status={selected.status} /> : <StatusChip status="info" />}
        </div>
        {selected ? (
          <>
            <h2>{selected.title}</h2>
            <p>{selected.subtitle}</p>
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
          </>
        ) : (
          <div className="cmd-empty small">
            <Boxes size={20} />
            <strong>Select a row</strong>
            <span>Record context lands here.</span>
          </div>
        )}
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
    </aside>
  );
}

export default function CommandHubClient() {
  const [payload, setPayload] = useState<CommandPayload | null>(null);
  const [error, setError] = useState<LoadFailure | null>(null);
  const [loading, setLoading] = useState(true);
  const [active, setActive] = useState<DomainId>('overview');
  const [query, setQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [selected, setSelected] = useState<CommandRow | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        const response = await fetch('/api/command', {
          headers: { Accept: 'application/json' },
          cache: 'no-store',
        });
        const body = await response.json().catch(() => ({}));
        if (!response.ok) {
          throw {
            status: response.status,
            message: str((body as Dict).error, `Command hub failed with ${response.status}`),
          };
        }
        if (!cancelled) {
          setPayload(body as CommandPayload);
          setError(null);
        }
      } catch (loadError) {
        if (!cancelled) {
          const failure = dict(loadError);
          setError({
            status: num(failure.status) || 500,
            message: loadError instanceof Error
              ? loadError.message
              : str(failure.message, 'Failed to load command hub'),
          });
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => {
      cancelled = true;
    };
  }, []);

  const systems = useMemo(() => systemHealth(payload), [payload]);
  const attention = useMemo(() => attentionQueue(payload, systems), [payload, systems]);
  const allRows = useMemo(() => rowsForDomain(payload, active), [payload, active]);
  const feedOps = dict(payload?.feedOps);
  const opsFeeds = list(feedOps.feeds);
  const opsRecent = list(feedOps.recentActivity);
  const opsPending = list(feedOps.pendingAhead);
  const opsFailures = list(feedOps.failures);
  const statuses = useMemo(() => {
    if (active === 'feeds') return ['all', 'smooth', 'pending', 'stale', 'failed', 'missing'];
    const set = new Set(allRows.map((row) => row.status).filter(Boolean));
    return ['all', ...Array.from(set).sort()];
  }, [active, allRows]);
  const rows = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return allRows.filter((row) => {
      const matchesStatus = statusFilter === 'all' || row.status === statusFilter;
      if (!matchesStatus) return false;
      if (!needle) return true;
      return [row.title, row.subtitle, row.status, row.source, row.eyebrow, row.metric]
        .join(' ')
        .toLowerCase()
        .includes(needle);
    });
  }, [allRows, query, statusFilter]);
  const visibleFeeds = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return opsFeeds.filter((feed) => {
      const matchesStatus = statusFilter === 'all' || str(feed.health) === statusFilter;
      if (!matchesStatus) return false;
      if (!needle) return true;
      return [feed.feedName, feed.status, feed.health, ...list(feed.feeders).map((feeder) => str(feeder.handle))]
        .join(' ')
        .toLowerCase()
        .includes(needle);
    });
  }, [opsFeeds, query, statusFilter]);
  const criticalCount = systems.filter((system) => system.status === 'failed').length
    + list(payload?.instrumentationGaps).filter((gap) => statusKind(gap.status) === 'failed').length;
  const betaState: 'READY' | 'WATCH' | 'BLOCKED' = criticalCount ? 'BLOCKED' : attention.length ? 'WATCH' : 'READY';
  const isAuthError = error?.status === 401;

  useEffect(() => {
    setSelected(rows[0] || null);
  }, [active, rows]);

  return (
    <div className="cmd-stage" data-command-shell>
      <div className="cmd-scan" aria-hidden="true" />
      <PortalHeader payload={payload} betaState={betaState} criticalCount={criticalCount} />
      <div className="cmd-refresh-line" aria-hidden="true" />

      <div className="cmd-shell">
        <aside className="cmd-rail">
          <div className="cmd-brand">
            <span className="cmd-brand-mark"><Lock size={15} /></span>
            <strong>COMMAND</strong>
          </div>
          <label className="cmd-search">
            <Search size={15} />
            <input value={query} onChange={(event) => setQuery(event.target.value)} placeholder="Search records" aria-label="Search command hub" />
          </label>
          {['command', 'domain'].map((group) => (
            <nav key={group} aria-label={group}>
              <span>{group}</span>
              {allNavItems.filter((item) => item.group === group).map((item) => {
                const Icon = item.icon;
                const health = systems.find((system) => system.id === item.id);
                return (
                  <button
                    key={item.id}
                    type="button"
                    className={active === item.id ? 'is-active' : ''}
                    onClick={() => {
                      setActive(item.id);
                      setStatusFilter('all');
                    }}
                  >
                    <Icon size={15} />
                    <em>{item.label}</em>
                    {health ? <i className={`cmd-dot is-${health.status}`} /> : null}
                  </button>
                );
              })}
            </nav>
          ))}
        </aside>

        <main className="cmd-main">
          {loading ? (
            <div className="cmd-loading-grid">
              {Array.from({ length: 10 }).map((_, index) => <span key={index} />)}
            </div>
          ) : error ? (
            <section className={`cmd-panel cmd-error ${isAuthError ? 'cmd-auth-error' : ''}`}>
              {isAuthError ? <ShieldCheck size={28} /> : <AlertTriangle size={28} />}
              <strong>{isAuthError ? 'Sign in required' : 'Command hub unavailable'}</strong>
              <span>
                {isAuthError
                  ? 'The command hub is read-only, but it still needs your FeedMe session before it can load account data.'
                  : error.message}
              </span>
              {isAuthError ? <a className="cmd-primary-link" href="/login?next=%2Fcommand">Sign in to Command Hub</a> : null}
            </section>
          ) : payload ? (
            <>
              <div className="cmd-mobile-tabs" aria-label="Command tabs">
                {navItems.map((item) => (
                  <button
                    key={item.id}
                    type="button"
                    className={active === item.id ? 'is-active' : ''}
                    onClick={() => {
                      setActive(item.id);
                      setStatusFilter('all');
                    }}
                  >
                    {item.label}
                  </button>
                ))}
              </div>

              <div className="cmd-content">
                <section className="cmd-center">
                  <BetaReadinessBand betaState={betaState} criticalCount={criticalCount} attention={attention} />

                  {active === 'overview' ? (
                    <>
                      <FeedOpsBoard feeds={visibleFeeds.slice(0, 8)} onSelect={setSelected} />
                      <div className="cmd-overview-grid">
                        <OpsEventList title="Recent App Activity" events={opsRecent.slice(0, 6)} empty="No recent activity" onSelect={setSelected} />
                        <OpsEventList title="Pending Ahead" events={opsPending.slice(0, 6)} empty="Nothing queued" onSelect={setSelected} />
                      </div>
                      <OpsEventList title="Failure Queue" events={opsFailures.slice(0, 6)} empty="No failures" onSelect={setSelected} />
                    </>
                  ) : active === 'feeds' ? (
                    <>
                      <div className="cmd-filter-row">
                        <span>Feeds / operations</span>
                        <label>
                          Health
                          <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                            {statuses.map((status) => <option key={status} value={status}>{labelize(status)}</option>)}
                          </select>
                        </label>
                      </div>
                      <FeedOpsBoard feeds={visibleFeeds} onSelect={setSelected} />
                    </>
                  ) : active === 'activity' ? (
                    <OpsEventList title="Recent App Activity" events={opsRecent} empty="No recent activity" onSelect={setSelected} />
                  ) : active === 'pending' ? (
                    <OpsEventList title="Pending Ahead" events={opsPending} empty="Nothing queued" onSelect={setSelected} />
                  ) : active === 'failures' ? (
                    <OpsEventList title="Failure Queue" events={opsFailures} empty="No failures" onSelect={setSelected} />
                  ) : active === 'gaps' ? (
                    <>
                      <GapChecklist gaps={list(payload.instrumentationGaps)} />
                      <CommandTable rows={rows} selected={selected} onSelect={setSelected} />
                    </>
                  ) : (
                    <>
                      <div className="cmd-filter-row">
                        <span>{allNavItems.find((item) => item.id === active)?.label} / records</span>
                        <label>
                          Status
                          <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                            {statuses.map((status) => <option key={status} value={status}>{labelize(status)}</option>)}
                          </select>
                        </label>
                      </div>
                      <PipelineRail active={active} payload={payload} />
                      <CommandTable rows={rows} selected={selected} onSelect={setSelected} />
                    </>
                  )}
                </section>

                <InspectorRail payload={payload} selected={selected} active={active} />
              </div>
            </>
          ) : null}
        </main>
      </div>
    </div>
  );
}
