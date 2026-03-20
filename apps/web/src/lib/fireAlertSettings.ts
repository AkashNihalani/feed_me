export type FireFilterThreshold = '10' | '25' | '50' | 'ALL';

export const FUND_ALERT_THRESHOLD_KEY = 'fund:alert-threshold';
export const PWA_NOTIFICATION_ENABLED_KEY = 'fund:pwa-notifications-enabled';

export function clampAlertThreshold(value: number, fallback = 25): number {
  if (!Number.isFinite(value)) return fallback;
  return Math.max(1, Math.min(100, Math.round(value)));
}

export function readFundAlertThreshold(storage?: Storage | null, fallback = 25): number {
  if (!storage) return fallback;
  const raw = storage.getItem(FUND_ALERT_THRESHOLD_KEY);
  if (!raw) return fallback;
  return clampAlertThreshold(Number(raw), fallback);
}

export function thresholdToFireFilter(threshold: number): FireFilterThreshold {
  const next = clampAlertThreshold(threshold);
  if (next <= 10) return '10';
  if (next <= 25) return '25';
  if (next <= 50) return '50';
  return 'ALL';
}

export function readBooleanFlag(storage: Storage | null | undefined, key: string): boolean {
  if (!storage) return false;
  return storage.getItem(key) === 'true';
}
