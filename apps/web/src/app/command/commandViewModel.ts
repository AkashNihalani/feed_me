export type CommandEvent = Record<string, unknown>;

export type CommandRunway = {
  allOpen: CommandEvent[];
  overdue: CommandEvent[];
  running: CommandEvent[];
  dueNext: CommandEvent[];
  later: CommandEvent[];
  dueWithinTwelveHours: CommandEvent[];
};

const TWELVE_HOURS_MS = 12 * 60 * 60 * 1000;

function text(value: unknown): string {
  return typeof value === 'string' ? value.trim() : '';
}
function timestamp(value: unknown): number {
  const parsed = typeof value === 'string' ? Date.parse(value) : Number.NaN;
  return Number.isFinite(parsed) ? parsed : 0;
}

export function commandEventTime(event: CommandEvent): string | null {
  return text(event.claimableAt)
    || text(event.dueAt)
    || text(event.scheduledAt)
    || text(event.nextRunAt)
    || text(event.happenedAt)
    || null;
}

function eventTimestamp(event: CommandEvent): number {
  return timestamp(commandEventTime(event));
}

function recencyTimestamp(event: CommandEvent): number {
  return timestamp(event.happenedAt) || eventTimestamp(event);
}

function uniqueEvents(events: CommandEvent[]): CommandEvent[] {
  const seen = new Set<string>();
  return events.filter((event, index) => {
    const id = text(event.id) || `${text(event.source)}:${text(event.kind)}:${index}`;
    if (seen.has(id)) return false;
    seen.add(id);
    return true;
  });
}

/**
 * Partitions the API's current operational state into the four visual runway
 * lanes. Every open event lands in exactly one lane and API queueState remains
 * the source of truth; the client only splits scheduled work at the 12h mark.
 */
export function partitionCommandRunway(
  events: CommandEvent[],
  nowMs = Date.now(),
): CommandRunway {
  const allOpen = uniqueEvents(events.filter((event) => event.isOpen === true));
  const overdue: CommandEvent[] = [];
  const running: CommandEvent[] = [];
  const dueNext: CommandEvent[] = [];
  const later: CommandEvent[] = [];
  const dueWithinTwelveHours: CommandEvent[] = [];
  const horizon = nowMs + TWELVE_HOURS_MS;

  for (const event of allOpen) {
    const queueState = text(event.queueState).toLowerCase();
    if (queueState === 'overdue') {
      overdue.push(event);
      continue;
    }
    if (queueState === 'in_progress') {
      running.push(event);
      continue;
    }
    if (queueState === 'scheduled') {
      const at = eventTimestamp(event);
      if (at > 0 && at <= horizon) {
        dueNext.push(event);
        dueWithinTwelveHours.push(event);
      } else {
        later.push(event);
      }
      continue;
    }
    if (queueState === 'queued') {
      dueNext.push(event);
      continue;
    }
    later.push(event);
  }

  overdue.sort((a, b) => eventTimestamp(a) - eventTimestamp(b));
  running.sort((a, b) => recencyTimestamp(b) - recencyTimestamp(a));
  dueNext.sort((a, b) => {
    const aTime = eventTimestamp(a) || Number.MAX_SAFE_INTEGER;
    const bTime = eventTimestamp(b) || Number.MAX_SAFE_INTEGER;
    return aTime - bTime;
  });
  later.sort((a, b) => {
    const aTime = eventTimestamp(a) || Number.MAX_SAFE_INTEGER;
    const bTime = eventTimestamp(b) || Number.MAX_SAFE_INTEGER;
    return aTime - bTime;
  });

  return { allOpen, overdue, running, dueNext, later, dueWithinTwelveHours };
}
