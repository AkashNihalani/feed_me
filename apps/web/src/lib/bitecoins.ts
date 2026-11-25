/* ─────────────────────────────────────────────────────────────
   BITECOINS — what a search and a file cost, and how many are
   left. A hundred a feeder, pooled, topped up to that every
   Tuesday; bought ones sit on top and never run out of time.

   The count lives on this device until the server keeps one: the
   same shape either way (the week's spending by day, two weeks
   before it), so Me, the rail and the dock won't change when it
   does. It is yours: it goes when you sign out (keepStore).
   ───────────────────────────────────────────────────────────── */

import { useEffect, useSyncExternalStore } from 'react';
import { ensureFeeds, getFeedsSnapshot, type AppFeed } from './feedsStore';
import { normalizeHandle } from './feedLabels';
import { onForget } from './keepStore';
import { addDays, istDayKey } from './postMoment';

export const PER_FEEDER = 100;

export type BitecoinEvent = {
  // a top up (Tuesday), bought, or spent: a search, a file followed, a file brought up to date
  t: 'top' | 'buy' | 'search' | 'follow' | 'update';
  n: number;
  // what was asked or followed, and about whom
  title?: string;
  h?: string;
  // a top up: what it came back to
  to?: number;
};

export type BitecoinWeek = { start: string; d: BitecoinEvent[][] };

export type BitecoinLedger = {
  v: 1;
  cap: number;
  weekly: number;
  bought: number;
  // what the card is out of
  full: number;
  // this week first, each from its Tuesday
  weeks: BitecoinWeek[];
};

const KEY = 'fm:bitecoins:v1';
const WEEKS_KEPT = 3;
const emptyDays = (): BitecoinEvent[][] => Array.from({ length: 7 }, () => []);

// the Tuesday this day's week began on (IST)
export function tuesdayOf(day: string): string {
  const weekday = new Date(`${day}T00:00:00Z`).getUTCDay();
  return addDays(day, -((weekday + 5) % 7));
}

export function dayOfWeek(week: string, day: string): number {
  return Math.min(6, Math.max(0, Math.round((Date.parse(`${day}T00:00:00Z`) - Date.parse(`${week}T00:00:00Z`)) / 86_400_000)));
}

let ledger: BitecoinLedger | null = null;
let read = false;
const listeners = new Set<() => void>();

function emit() {
  listeners.forEach((listener) => listener());
}

function readLedger() {
  if (read || typeof window === 'undefined') return;
  read = true;
  try {
    const kept = JSON.parse(window.localStorage.getItem(KEY) || 'null') as BitecoinLedger | null;
    if (kept && kept.v === 1 && Array.isArray(kept.weeks) && kept.weeks.length && Number.isFinite(kept.weekly)) ledger = kept;
  } catch {
    // nothing kept, or storage blocked
  }
}

function keep(next: BitecoinLedger) {
  ledger = next;
  try {
    window.localStorage.setItem(KEY, JSON.stringify(next));
  } catch {
    // storage blocked: it holds until the app closes
  }
  emit();
}

if (typeof window !== 'undefined') {
  onForget(() => {
    ledger = null;
    read = true;
    try {
      window.localStorage.removeItem(KEY);
    } catch {
      // storage blocked
    }
    emit();
  });
}

/* Brought up to today: a new week topped up to its cap, and a feeder added or taken away since moves what there is by
   its hundred */
export function syncBitecoins(feeders: number) {
  readLedger();
  const cap = feeders * PER_FEEDER;
  const week = tuesdayOf(istDayKey(Date.now()));
  if (!ledger) {
    keep({ v: 1, cap, weekly: cap, bought: 0, full: cap, weeks: [{ start: week, d: emptyDays() }] });
    return;
  }
  let next = ledger;
  if (next.cap !== cap) {
    const weekly = Math.max(0, next.weekly + cap - next.cap);
    next = { ...next, cap, weekly, full: Math.max(cap + next.bought, weekly + next.bought) };
  }
  if (next.weeks[0].start < week) {
    const top = Math.max(0, cap - next.weekly);
    const days = emptyDays();
    if (top) days[0].push({ t: 'top', n: top, to: cap });
    next = { ...next, weekly: cap, full: cap + next.bought, weeks: [{ start: week, d: days }, ...next.weeks].slice(0, WEEKS_KEPT) };
  }
  if (next !== ledger) keep(next);
}

export function getBitecoins(): BitecoinLedger | null {
  readLedger();
  return ledger;
}

export function subscribeBitecoins(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function feederCount(feeds: AppFeed[] | null): number | null {
  if (!feeds) return null;
  return new Set(feeds.flatMap((feed) => feed.feeders.map((feeder) => normalizeHandle(feeder.handle))).filter(Boolean)).size;
}

const ASK_AFTER_MS = 4000;

/* How many are left, for whatever shows it (null until it is known). It goes by the feeds already on hand; a device that
   has never counted asks for them once the page it opened on has had its turn */
export function useBitecoins(): number | null {
  useEffect(() => {
    const onHand = () => {
      const count = feederCount(getFeedsSnapshot());
      if (count != null) syncBitecoins(count);
      return count != null;
    };
    if (onHand()) return undefined;
    const kept = getBitecoins();
    if (kept) {
      syncBitecoins(kept.cap / PER_FEEDER);
      return undefined;
    }
    const timer = window.setTimeout(() => void ensureFeeds().then(onHand), ASK_AFTER_MS);
    return () => window.clearTimeout(timer);
  }, []);
  const now = useSyncExternalStore(subscribeBitecoins, getBitecoins, () => null);
  return now ? Math.max(0, Math.round(now.weekly + now.bought)) : null;
}

export const bitecoinsLabel = (balance: number | null) => (balance == null ? 'Bitecoins' : `${balance.toLocaleString('en-US')} Bitecoins`);
