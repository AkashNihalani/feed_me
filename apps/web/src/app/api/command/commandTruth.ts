const IST_OFFSET_MINUTES = 5 * 60 + 30;

const OPEN_STATUSES = new Set([
  'pending',
  'retry',
  'running',
  'pending_capture',
  'capturing',
  'purge_pending',
  'purging',
]);

const IN_PROGRESS_STATUSES = new Set(['running', 'capturing', 'purging']);

export type QueueState = 'scheduled' | 'overdue' | 'in_progress' | 'queued' | null;

export type OperationalEvent = {
  id: string;
  source: string;
  kind: string;
  status: string | null;
  feedId: number | null;
  feederId: number | null;
  postKey: string | null;
  happenedAt: string | null;
  nextRunAt: string | null;
  scheduledAt: string | null;
  dueAt: string | null;
  claimableAt: string | null;
  isOperational: boolean;
  isOpen: boolean;
  overdue: boolean;
  queueState: QueueState;
};

export function dateMs(value: unknown): number {
  if (typeof value !== 'string' || !value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function istDateParts(value: string) {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) return null;

  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date);
  const byType = new Map(parts.map((part) => [part.type, part.value]));
  const year = Number(byType.get('year'));
  const month = Number(byType.get('month'));
  const day = Number(byType.get('day'));
  const hour = Number(byType.get('hour'));
  const minute = Number(byType.get('minute'));

  if (![year, month, day, hour, minute].every(Number.isFinite)) return null;
  return { year, month, day, hour, minute };
}

function istLocalToIso(
  year: number,
  month: number,
  day: number,
  hour: number,
  addDays: number,
): string {
  const localCalendarMs = Date.UTC(year, month - 1, day + addDays, hour, 0, 0, 0);
  return new Date(localCalendarMs - IST_OFFSET_MINUTES * 60 * 1000).toISOString();
}

/**
 * Mirrors fn_checkpoint_job_claimable from the two-discovery pipeline.
 * A first checkpoint attempt is collected by the discovery slot after its
 * nominal due time; retries are claimable at next_run_at itself.
 */
export function checkpointClaimableAt(dueAt: unknown, attempt: unknown): string | null {
  if (typeof dueAt !== 'string' || !dateMs(dueAt)) return null;
  const parsedAttempt = typeof attempt === 'number' ? attempt : Number(attempt || 0);
  if (Number.isFinite(parsedAttempt) && parsedAttempt > 0) {
    return new Date(dateMs(dueAt)).toISOString();
  }

  const parts = istDateParts(dueAt);
  if (!parts) return null;
  const minutes = parts.hour * 60 + parts.minute;
  if (minutes <= 11 * 60 + 30) {
    return istLocalToIso(parts.year, parts.month, parts.day, 12, 0);
  }
  if (minutes <= 23 * 60 + 30) {
    return istLocalToIso(parts.year, parts.month, parts.day, 0, 1);
  }
  return istLocalToIso(parts.year, parts.month, parts.day, 12, 1);
}

export function queueTiming(
  status: unknown,
  timing: {
    scheduledAt?: string | null;
    dueAt?: string | null;
    claimableAt?: string | null;
  },
  nowMs = Date.now(),
) {
  const normalizedStatus = typeof status === 'string' ? status.trim().toLowerCase() : '';
  const isOpen = OPEN_STATUSES.has(normalizedStatus);
  const isInProgress = IN_PROGRESS_STATUSES.has(normalizedStatus);
  const actionableAt = timing.claimableAt || timing.dueAt || timing.scheduledAt || null;
  const actionableMs = dateMs(actionableAt);
  const overdue = isOpen && !isInProgress && actionableMs > 0 && actionableMs <= nowMs;
  const queueState: QueueState = !isOpen
    ? null
    : isInProgress
      ? 'in_progress'
      : overdue
        ? 'overdue'
        : actionableMs > nowMs
          ? 'scheduled'
          : 'queued';

  return { isOpen, overdue, queueState, actionableAt };
}

function eventRecency(event: OperationalEvent): number {
  return Math.max(
    dateMs(event.happenedAt),
    dateMs(event.claimableAt),
    dateMs(event.dueAt),
    dateMs(event.scheduledAt),
  );
}

function laneKey(event: OperationalEvent): string {
  const owner = event.feederId != null
    ? `feeder:${event.feederId}`
    : event.feedId != null
      ? `feed:${event.feedId}`
      : event.postKey
        ? `post:${event.postKey}`
        : `event:${event.id}`;
  return `${event.source}:${event.kind}:${owner}`;
}

/** A later result in the same operational lane supersedes older failures. */
export function currentOperationalStates<T extends OperationalEvent>(events: T[]): T[] {
  const latestByLane = new Map<string, T>();
  for (const event of events) {
    if (!event.isOperational) continue;
    const key = laneKey(event);
    const current = latestByLane.get(key);
    if (!current || eventRecency(event) > eventRecency(current)) {
      latestByLane.set(key, event);
    }
  }
  return Array.from(latestByLane.values()).sort((a, b) => eventRecency(b) - eventRecency(a));
}
