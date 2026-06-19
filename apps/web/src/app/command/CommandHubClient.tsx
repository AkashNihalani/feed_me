'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  AlertTriangle,
  Bell,
  Boxes,
  Brain,
  ChartNoAxesColumnIncreasing,
  ChevronDown,
  CircleDollarSign,
  Flame,
  Gauge,
  HardDrive,
  Layers3,
  Network,
  Search,
  ShieldCheck,
  Sparkles,
  TimerReset,
  WalletCards,
} from 'lucide-react';

type Dict = Record<string, unknown>;
type DomainId =
  | 'overview'
  | 'account'
  | 'engine'
  | 'checkpoints'
  | 'fire'
  | 'intelligence'
  | 'media'
  | 'notifications'
  | 'finance'
  | 'gaps';

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

type NavItem = {
  id: DomainId;
  label: string;
  icon: typeof Gauge;
  group: 'main' | 'pipeline' | 'stack';
};

type CommandPayload = {
  generatedAt?: string;
  access?: Dict;
  pricing?: Dict;
  topline?: Dict;
  accountGraph?: Dict;
  engine?: Dict;
  checkpoints?: Dict;
  fireSignals?: Dict;
  intelligence?: Dict;
  media?: Dict;
  notifications?: Dict;
  finance?: Dict;
  instrumentationGaps?: Dict[];
};

type LoadFailure = {
  status: number;
  message: string;
};

const navItems: NavItem[] = [
  { id: 'overview', label: 'Dashboard', icon: Gauge, group: 'main' },
  { id: 'account', label: 'Account graph', icon: Network, group: 'main' },
  { id: 'engine', label: 'Engine', icon: TimerReset, group: 'main' },
  { id: 'checkpoints', label: 'Checkpoints', icon: Layers3, group: 'pipeline' },
  { id: 'fire', label: 'Fire / Signals', icon: Flame, group: 'pipeline' },
  { id: 'intelligence', label: 'Intelligence', icon: Brain, group: 'pipeline' },
  { id: 'media', label: 'Media', icon: HardDrive, group: 'stack' },
  { id: 'notifications', label: 'Notifications', icon: Bell, group: 'stack' },
  { id: 'finance', label: 'Finance', icon: CircleDollarSign, group: 'stack' },
  { id: 'gaps', label: 'Instrumentation gaps', icon: AlertTriangle, group: 'stack' },
];

const statusTone: Record<string, string> = {
  active: 'good',
  fresh: 'good',
  done: 'good',
  sent: 'good',
  paid: 'good',
  pending: 'watch',
  retry: 'watch',
  running: 'blue',
  capturing: 'blue',
  partial: 'blue',
  paused: 'muted',
  skipped: 'muted',
  stale: 'muted',
  configuration_needed: 'watch',
  missing_instrumentation: 'watch',
  failed: 'bad',
  error: 'bad',
  capture_failed: 'bad',
  purge_failed: 'bad',
  unavailable: 'bad',
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

function shortDate(value: unknown) {
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
  }).format(date);
}

function relativeTime(value: unknown) {
  const raw = str(value);
  if (!raw) return 'No timestamp';
  const date = new Date(raw);
  if (Number.isNaN(date.getTime())) return 'No timestamp';
  const diff = Date.now() - date.getTime();
  const minutes = Math.round(diff / 60000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 48) return `${hours}h ago`;
  const days = Math.round(hours / 24);
  return `${days}d ago`;
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
        subtitle: `Feed #${str(row.feed_id)} · ${formatNumber(row.follower_count)} followers`,
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
      title: `${labelize(row.job_type, 'Run')} · feeder #${str(row.feeder_id)}`,
      eyebrow: 'Run job',
      subtitle: str(row.last_error, `Business day ${str(row.business_date_ist, 'not set')}`),
      metric: `attempt ${formatNumber(row.attempt)}`,
      source: 'run_jobs',
    }));
  }

  if (domain === 'checkpoints') {
    return [
      ...list(checkpoints.recentJobs).map((row) => makeRow(row, {
        title: `${labelize(row.checkpoint, 'Checkpoint')} · ${str(row.post_key, 'post')}`,
        eyebrow: 'Checkpoint job',
        subtitle: str(row.last_error, `Next run ${shortDate(row.next_run_at)}`),
        metric: `attempt ${formatNumber(row.attempt)}`,
        source: 'checkpoint_jobs',
      })),
      ...list(checkpoints.recentMetrics).map((row) => makeRow(row, {
        title: `${labelize(row.checkpoint, 'Metric')} · ${str(row.post_key, 'post')}`,
        eyebrow: 'Metric surface',
        subtitle: `Likes ${formatNumber(row.likes)} · Comments ${formatNumber(row.comments)} · Views ${formatNumber(row.views)}`,
        status: 'done',
        metric: str(row.percentile_performance, 'percentile'),
        updatedAt: str(row.computed_at),
        source: 'post_metrics',
      })),
    ];
  }

  if (domain === 'fire') {
    return [
      ...list(fire.recentAlerts).map((row) => makeRow(row, {
        title: `${labelize(row.alert_type, 'Alert')} · ${str(row.post_key, 'post')}`,
        eyebrow: labelize(row.signal_code, 'Fire alert'),
        subtitle: str(row.body, `${labelize(row.checkpoint, 'checkpoint')} · ${str(row.metric_key, 'metric')}`),
        metric: str(row.surface_percentile, 'surface'),
        source: 'fire_alerts',
      })),
      ...list(fire.recentSignals).map((row) => makeRow(row, {
        title: labelize(row.signal_type, 'Signal'),
        eyebrow: labelize(row.signal_family, 'Signal family'),
        subtitle: str(row.body, `${labelize(row.scope, 'scope')} · ${labelize(row.checkpoint, 'checkpoint')}`),
        metric: str(row.business_date_ist),
        updatedAt: str(row.last_fired_at) || str(row.updated_at),
        source: 'signals',
      })),
    ];
  }

  if (domain === 'intelligence') {
    return [
      ...list(intelligence.recentModelCalls).map((row) => makeRow(row, {
        title: `${labelize(row.call_type, 'Model call')} · ${str(row.feeder_handle, 'unknown')}`,
        eyebrow: str(row.model, 'model'),
        subtitle: str(row.error, `${str(row.prompt_version, 'prompt')} · ${str(row.post_key, 'no post')}`),
        metric: str(row.pattern_id, str(row.call_key, 'audit')),
        updatedAt: str(row.completed_at) || str(row.updated_at),
        source: 'feeder_file_model_calls',
      })),
      ...list(artifacts.feederFiles).map((row) => makeRow(row, {
        title: str(row.feeder_handle, 'Feeder file'),
        eyebrow: 'Feeder file',
        subtitle: `${str(row.compile_version, 'compile')} · ${str(row.active_window, 'window unknown')}`,
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
      title: `${labelize(row.asset_role, 'Asset')} · ${str(row.post_key, 'post')}`,
      eyebrow: str(row.storage_bucket, 'storage'),
      subtitle: str(row.last_error, `${str(row.mime_type, 'mime unknown')} · ${formatBytes(row.byte_size)}`),
      metric: formatBytes(row.byte_size),
      source: 'post_media_assets',
    }));
  }

  if (domain === 'notifications') {
    return [
      ...list(notifications.recentJobs).map((row) => makeRow(row, {
        title: `${labelize(row.kind, 'Push')} job · ${str(row.dedupe_key, 'dedupe')}`,
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
    ...rowsForDomain(payload, 'engine').slice(0, 6),
    ...rowsForDomain(payload, 'fire').slice(0, 5),
    ...rowsForDomain(payload, 'intelligence').slice(0, 5),
    ...rowsForDomain(payload, 'media').slice(0, 4),
    ...rowsForDomain(payload, 'gaps').slice(0, 4),
  ];
}

function MetricCard({
  label,
  value,
  delta,
  color,
  bars = 16,
}: {
  label: string;
  value: string;
  delta: string;
  color: 'blue' | 'green' | 'orange' | 'violet';
  bars?: number;
}) {
  const filled = Math.max(4, Math.min(bars - 2, Math.round((bars * (color === 'orange' ? 0.74 : color === 'green' ? 0.63 : 0.56)))));

  return (
    <section className="cmd-card cmd-metric-card">
      <div className="cmd-card-label">
        <Sparkles size={13} />
        <span>{label}</span>
      </div>
      <div className="cmd-metric-value">{value}</div>
      <div className={`cmd-delta cmd-delta-${color}`}>{delta}</div>
      <div className="cmd-micro-bars" aria-hidden="true">
        {Array.from({ length: bars }).map((_, index) => (
          <span key={index} className={index < filled ? `is-${color}` : ''} />
        ))}
      </div>
    </section>
  );
}

function MiniBarChart({
  payload,
  active,
  onSelect,
}: {
  payload: CommandPayload;
  active: DomainId;
  onSelect: (domain: DomainId) => void;
}) {
  const topline = dict(payload.topline);
  const engineTotals = dict(dict(payload.engine).totals);
  const checkpointTotals = dict(dict(payload.checkpoints).totals);
  const fireTotals = dict(dict(payload.fireSignals).totals);
  const intelligenceTotals = dict(dict(payload.intelligence).totals);
  const mediaTotals = dict(dict(payload.media).totals);
  const notificationTotals = dict(dict(payload.notifications).totals);
  const gaps = list(payload.instrumentationGaps).length;

  const bars: { id: DomainId; label: string; value: number; color?: string }[] = [
    { id: 'account', label: 'Accounts', value: num(topline.feeders) },
    { id: 'engine', label: 'Engine', value: num(engineTotals.jobs), color: '#139df2' },
    { id: 'checkpoints', label: 'D1-D21', value: num(checkpointTotals.jobs) },
    { id: 'fire', label: 'Fire', value: num(fireTotals.alerts) + num(fireTotals.signals), color: '#ff6a00' },
    { id: 'intelligence', label: 'Intel', value: num(intelligenceTotals.modelCalls) + num(intelligenceTotals.feederFiles), color: '#675cff' },
    { id: 'media', label: 'Media', value: num(mediaTotals.assets) },
    { id: 'notifications', label: 'Push', value: num(notificationTotals.jobs) + num(notificationTotals.subscriptions) },
    { id: 'finance', label: 'Finance', value: Math.max(1, num(topline.plannedMonthlyRevenueInr) / 1499), color: '#18bf73' },
    { id: 'gaps', label: 'Gaps', value: gaps, color: '#ff4f7b' },
  ];
  const max = Math.max(1, ...bars.map((bar) => bar.value));

  return (
    <section className="cmd-card cmd-workload">
      <div className="cmd-section-head">
        <div>
          <div className="cmd-card-label">
            <ChartNoAxesColumnIncreasing size={14} />
            <span>System workload</span>
          </div>
          <div className="cmd-workload-title">{formatNumber(num(topline.posts))}</div>
          <span className="cmd-muted">tracked posts across read-only contracts</span>
        </div>
        <div className="cmd-select-chip">
          Today IST
          <ChevronDown size={14} />
        </div>
      </div>
      <div className="cmd-bars-grid">
        {bars.map((bar) => {
          const height = 18 + (bar.value / max) * 174;
          const isActive = active === bar.id;
          return (
            <button
              key={bar.id}
              type="button"
              className={`cmd-bar-button ${isActive ? 'is-active' : ''}`}
              onClick={() => onSelect(bar.id)}
              aria-label={`View ${bar.label}`}
            >
              <span className="cmd-bar-value">{formatNumber(bar.value)}</span>
              <span
                className="cmd-bar"
                style={{
                  height,
                  background: isActive ? (bar.color || '#139df2') : undefined,
                }}
              />
              <span className="cmd-bar-label">{bar.label}</span>
            </button>
          );
        })}
      </div>
    </section>
  );
}

function StatusChip({ status }: { status: string }) {
  const key = status.toLowerCase();
  return <span className={`cmd-status is-${statusTone[key] || 'muted'}`}>{labelize(status)}</span>;
}

function DataTable({
  rows,
  selected,
  onSelect,
}: {
  rows: CommandRow[];
  selected: CommandRow | null;
  onSelect: (row: CommandRow) => void;
}) {
  return (
    <section className="cmd-card cmd-table-card">
      <div className="cmd-table-head">
        <div className="cmd-card-label">
          <Boxes size={14} />
          <span>Read-only record ledger</span>
        </div>
        <span>{rows.length} visible</span>
      </div>
      {rows.length === 0 ? (
        <div className="cmd-empty">
          <ShieldCheck size={24} />
          <strong>No records found</strong>
          <span>The selected scope has no rows for this view.</span>
        </div>
      ) : (
        <div className="cmd-table-wrap">
          <table className="cmd-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Source</th>
                <th>Status</th>
                <th>Metric</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr
                  key={`${row.source}:${row.id}`}
                  className={selected?.id === row.id && selected.source === row.source ? 'is-selected' : ''}
                  onClick={() => onSelect(row)}
                >
                  <td>
                    <div className="cmd-row-title">{row.title}</div>
                    <div className="cmd-row-subtitle">{row.subtitle}</div>
                  </td>
                  <td>{row.source}</td>
                  <td><StatusChip status={row.status} /></td>
                  <td>{row.metric || row.eyebrow}</td>
                  <td>{relativeTime(row.updatedAt)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}

function FinanceRail({ payload, selected }: { payload: CommandPayload; selected: CommandRow | null }) {
  const finance = dict(payload.finance);
  const assumptions = dict(finance.assumptions);
  const realSurfaces = dict(finance.realSurfaces);
  const topline = dict(payload.topline);
  const gaps = list(payload.instrumentationGaps);
  const preview = selected?.raw || {};
  const previewEntries = Object.entries(preview)
    .filter(([, value]) => value == null || ['string', 'number', 'boolean'].includes(typeof value))
    .slice(0, 8);

  return (
    <aside className="cmd-right-rail">
      <section className="cmd-card cmd-pricing-card">
        <div className="cmd-card-label">
          <WalletCards size={14} />
          <span>Finance assumptions</span>
        </div>
        <div className="cmd-metal-card" aria-hidden="true">
          <span>FEEDME</span>
          <strong>COMMAND</strong>
          <small>read-only ledger</small>
        </div>
        <div className="cmd-rail-list">
          <div><span>Price per feeder</span><strong>{formatInr(assumptions.plannedPriceInrPerFeeder)}</strong></div>
          <div><span>Razorpay</span><strong>not live</strong></div>
          <div><span>Bright Data</span><strong>$1.50 / 1k</strong></div>
          <div><span>Planned MRR</span><strong>{formatInr(topline.plannedMonthlyRevenueInr)}</strong></div>
          <div><span>Known paid</span><strong>{formatInr(realSurfaces.paidTransactionsInr)}</strong></div>
        </div>
      </section>

      <section className="cmd-card cmd-detail-card">
        <div className="cmd-section-head compact">
          <div className="cmd-card-label">
            <Sparkles size={14} />
            <span>Selection detail</span>
          </div>
          {selected ? <StatusChip status={selected.status} /> : null}
        </div>
        {selected ? (
          <>
            <h2>{selected.title}</h2>
            <p>{selected.subtitle}</p>
            <div className="cmd-rail-list compact-list">
              <div><span>Source</span><strong>{selected.source}</strong></div>
              <div><span>Updated</span><strong>{shortDate(selected.updatedAt)}</strong></div>
              <div><span>Metric</span><strong>{selected.metric || selected.eyebrow}</strong></div>
              {previewEntries.map(([key, value]) => (
                <div key={key}><span>{labelize(key)}</span><strong>{value == null ? 'null' : String(value)}</strong></div>
              ))}
            </div>
          </>
        ) : (
          <div className="cmd-empty small">
            <Boxes size={20} />
            <strong>Select a row</strong>
            <span>Record details appear here without exposing mutations.</span>
          </div>
        )}
      </section>

      <section className="cmd-card cmd-gaps-card">
        <div className="cmd-section-head compact">
          <div className="cmd-card-label">
            <AlertTriangle size={14} />
            <span>Instrumentation gaps</span>
          </div>
          <strong>{gaps.length}</strong>
        </div>
        <div className="cmd-gap-bars" aria-hidden="true">
          {gaps.slice(0, 28).map((gap, index) => (
            <span key={`${str(gap.id, 'gap')}-${index}`} className={`is-${index % 3}`} />
          ))}
        </div>
        <div className="cmd-gap-list">
          {gaps.slice(0, 4).map((gap) => (
            <div key={str(gap.id, str(gap.label))}>
              <span>{str(gap.label, 'Gap')}</span>
              <strong>{labelize(gap.status, 'Missing')}</strong>
            </div>
          ))}
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

  const allRows = useMemo(() => rowsForDomain(payload, active), [payload, active]);
  const statuses = useMemo(() => {
    const set = new Set(allRows.map((row) => row.status).filter(Boolean));
    return ['all', ...Array.from(set).sort()];
  }, [allRows]);
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

  useEffect(() => {
    setSelected(rows[0] || null);
  }, [active, rows]);

  const topline = dict(payload?.topline);
  const engineTotals = dict(dict(payload?.engine).totals);
  const fireTotals = dict(dict(payload?.fireSignals).totals);
  const mediaTotals = dict(dict(payload?.media).totals);
  const access = dict(payload?.access);
  const activeNav = navItems.find((item) => item.id === active);
  const isAuthError = error?.status === 401;

  return (
    <div className="cmd-stage" data-command-shell>
      <div className="cmd-title">FeedMe Command</div>
      <div className="cmd-shell">
        <aside className="cmd-sidebar">
          <div className="cmd-brand">
            <span className="cmd-brand-mark"><Sparkles size={17} /></span>
            <strong>FeedMe Inc.</strong>
          </div>
          <label className="cmd-search">
            <Search size={16} />
            <input
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search"
              aria-label="Search command hub"
            />
            <span>⌘P</span>
          </label>

          {(['main', 'pipeline', 'stack'] as const).map((group) => (
            <nav key={group} className="cmd-nav-group" aria-label={group}>
              <div className="cmd-nav-label">
                {group === 'main' ? 'Main Menu' : group === 'pipeline' ? 'Pipeline' : 'Stack'}
                <ChevronDown size={13} />
              </div>
              {navItems.filter((item) => item.group === group).map((item) => {
                const Icon = item.icon;
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
                    <Icon size={16} />
                    <span>{item.label}</span>
                    {item.id === 'gaps' ? <em>{list(payload?.instrumentationGaps).length}</em> : null}
                  </button>
                );
              })}
            </nav>
          ))}

          <div className="cmd-upgrade">
            <span className="cmd-brand-mark"><ShieldCheck size={16} /></span>
            <strong>Read-only</strong>
            <p>{str(access.note, 'Signed-in command scope')}</p>
          </div>
          <div className="cmd-user">
            <span>{str(access.signedInEmail, 'Admin')}</span>
            <small>{labelize(access.mode, 'Account scope')}</small>
          </div>
        </aside>

        <main className="cmd-main">
          <header className="cmd-header">
            <div>
              <h1>{activeNav?.label || 'Dashboard'}</h1>
              <span>{payload ? `Last read ${shortDate(payload.generatedAt)}` : 'Preparing read model'}</span>
            </div>
            <div className="cmd-header-actions">
              <span className="cmd-black-chip"><ShieldCheck size={15} /> Read-only</span>
              <span className="cmd-soft-chip">{labelize(access.mode, loading ? 'Loading' : 'Scoped')}</span>
            </div>
          </header>

          {loading ? (
            <div className="cmd-loading-grid">
              {Array.from({ length: 8 }).map((_, index) => <span key={index} />)}
            </div>
          ) : error ? (
            <section className={`cmd-card cmd-error ${isAuthError ? 'cmd-auth-error' : ''}`}>
              {isAuthError ? <ShieldCheck size={28} /> : <AlertTriangle size={28} />}
              <strong>{isAuthError ? 'Sign in required' : 'Command hub unavailable'}</strong>
              <span>
                {isAuthError
                  ? 'The command hub is read-only, but it still needs your FeedMe session before it can load account data.'
                  : error.message}
              </span>
              {isAuthError ? (
                <a className="cmd-primary-link" href="/login?next=%2Fcommand">
                  Sign in to Command Hub
                </a>
              ) : null}
            </section>
          ) : payload ? (
            <>
              <section className="cmd-metrics-grid">
                <MetricCard
                  label="Account graph"
                  value={formatNumber(topline.activeFeeders)}
                  delta={`${formatNumber(topline.followerReach)} follower reach`}
                  color="blue"
                />
                <MetricCard
                  label="Engine jobs"
                  value={formatNumber(engineTotals.jobs)}
                  delta={`${formatNumber(engineTotals.successPercent)}% success`}
                  color="green"
                />
                <MetricCard
                  label="Fire / Signals"
                  value={formatNumber(num(fireTotals.alerts) + num(fireTotals.signals))}
                  delta={`${formatNumber(fireTotals.hotPosts)} hot posts`}
                  color="orange"
                />
                <MetricCard
                  label="Media storage"
                  value={formatBytes(mediaTotals.knownBytes)}
                  delta={`${formatNumber(mediaTotals.failed)} failed captures`}
                  color="violet"
                />
              </section>

              <div className="cmd-content-grid">
                <div className="cmd-center">
                  <MiniBarChart payload={payload} active={active} onSelect={setActive} />
                  <div className="cmd-filter-row">
                    <span>{activeNav?.label || 'Dashboard'} records</span>
                    <label>
                      Status
                      <select value={statusFilter} onChange={(event) => setStatusFilter(event.target.value)}>
                        {statuses.map((status) => <option key={status} value={status}>{labelize(status)}</option>)}
                      </select>
                    </label>
                  </div>
                  <DataTable rows={rows} selected={selected} onSelect={setSelected} />
                </div>
                <FinanceRail payload={payload} selected={selected} />
              </div>
            </>
          ) : null}
        </main>
      </div>
    </div>
  );
}
