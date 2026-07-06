import { FireItem } from './types';
import {
  CANONICAL_FIRE_METRICS,
  formatMetricValue,
  metricLabel,
  resolveBestMetricFromPayload,
} from './fireMetricDisplay';

type LayerSlide = {
  title: string;
  rows: string[];
};

export function compact(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  const n = Math.abs(v);
  if (n >= 1_000_000_000) return `${(v / 1_000_000_000).toFixed(2)}B`;
  if (n >= 1_000_000) return `${(v / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(v / 1_000).toFixed(1)}K`;
  return String(Math.round(v));
}

export function pct(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  return `${Math.round(v)}%`;
}

export function multiple(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  return `${v.toFixed(2)}x`;
}

export function hourLabel(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return 'INSUFFICIENT DATA';
  const hh = String(Math.max(0, Math.min(23, Math.round(v)))).padStart(2, '0');
  return `${hh}:00`;
}

export function surfaceTone(trajectoryDelta: number | null): { percentileColor: string; deltaColor: string; deltaText: string | null } {
  if (trajectoryDelta == null || !Number.isFinite(trajectoryDelta)) {
    return { percentileColor: '#FFFFFF', deltaColor: '#FFFFFF', deltaText: null };
  }

  const x = Math.round(trajectoryDelta);
  if (x > 0) {
    // Lower percentile is better -> positive shift is shown as a green down arrow.
    return { percentileColor: '#C8FF1A', deltaColor: '#C8FF1A', deltaText: `↓${x}` };
  }
  if (x < 0) {
    // Higher percentile is worse -> negative shift is shown as red up arrow.
    return { percentileColor: '#FF3B30', deltaColor: '#FF3B30', deltaText: `↑${Math.abs(x)}` };
  }
  return { percentileColor: '#FFFFFF', deltaColor: '#FFFFFF', deltaText: null };
}

export function buildSlides(item: FireItem): LayerSlide[] {
  const payload = (item.payload ?? {}) as Record<string, unknown>;
  const asRec = (v: unknown): Record<string, unknown> => (v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : {});

  const metrics = asRec(payload.metrics);
  const position = asRec(payload.position);
  const timing = asRec(payload.timing);
  const trajectory = asRec(payload.trajectory);

  const bestMetric = resolveBestMetricFromPayload(
    metrics,
    typeof payload.best_metric === 'string' && payload.best_metric.trim() ? payload.best_metric.trim().toLowerCase() : 'engagement_rate',
  );

  const metricRows = CANONICAL_FIRE_METRICS.map((m) => {
    const md = asRec(metrics[m]);
    const val = formatMetricValue(m, typeof md.value === 'number' ? md.value : null);
    const base = formatMetricValue(m, typeof md.baseline === 'number' ? md.baseline : null);
    const mult = multiple(typeof md.multiple === 'number' ? md.multiple : null);
    return `${metricLabel(m, 'short').toUpperCase()} ${val} · ${base} · ${mult}`;
  });

  const bestData = asRec(metrics[bestMetric]);
  const rankFeed = typeof bestData.rank_feed === 'number' ? bestData.rank_feed : (typeof position.feed_rank === 'number' ? position.feed_rank : null);
  const bestInN = typeof bestData.best_in_last_n === 'number' ? bestData.best_in_last_n : null;

  const posRows: string[] = [];
  posRows.push(rankFeed != null ? `FEED #${Math.round(rankFeed)}` : 'FEED RANK —');
  posRows.push(bestInN != null ? `FEEDER ${Math.max(1, Math.round(bestInN))}/50` : 'FEEDER RECENT —');

  const cp = (item.checkpoint || 'D1').toUpperCase();
  const l3Rows: string[] = [];

  if (cp === 'D1') {
    const th = typeof timing.hour === 'number' ? timing.hour : null;
    const hm = typeof timing.hour_multiple === 'number' ? timing.hour_multiple : null;

    l3Rows.push(`HOUR ${hourLabel(th)}`);
    if (hm != null) l3Rows.push(`TIME LIFT ${hm.toFixed(2)}X`);
  } else {
    const d1 = typeof trajectory.d1 === 'number' ? trajectory.d1 : null;
    const d3 = typeof trajectory.d3 === 'number' ? trajectory.d3 : null;
    const d7 = typeof trajectory.d7 === 'number' ? trajectory.d7 : null;
    const delta = typeof trajectory.delta === 'number' ? trajectory.delta : null;

    l3Rows.push(`D1 ${d1 == null ? '--' : Math.round(d1)}`);
    l3Rows.push(`D3 ${d3 == null ? '--' : Math.round(d3)}`);
    l3Rows.push(`D7 ${d7 == null ? '--' : Math.round(d7)}`);
    l3Rows.push(`Δ ${delta == null ? '--' : Math.round(delta)}`);
  }

  return [
    { title: 'L1 METRICS', rows: metricRows },
    { title: 'L2 POSITION', rows: posRows },
    { title: cp === 'D1' ? 'L3 TIMING' : 'L3 TRAJECTORY', rows: l3Rows },
  ];
}
