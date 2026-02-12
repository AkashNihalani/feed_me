import { type ClassValue, clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function extractHandle(url: string, platform: string): string {
  try {
    const cleanUrl = url.replace(/\/$/, '').replace(/\?.*$/, ''); // Remove trailing slash and query params
    
    if (platform === 'x' || platform === 'twitter') {
      const match = cleanUrl.match(/(?:x\.com|twitter\.com)\/([^\/]+)|^@?([a-zA-Z0-9_]+)$/);
      return match ? `@${match[1] || match[2]}` : url;
    }
    
    if (platform === 'instagram') {
      const match = cleanUrl.match(/instagram\.com\/([^\/]+)/);
      return match ? `@${match[1]}` : url;
    }
    
    if (platform === 'linkedin') {
      const match = cleanUrl.match(/linkedin\.com\/in\/([^\/]+)/);
      return match ? match[1] : (cleanUrl.split('/').pop() || url);
    }

    if (platform === 'youtube') {
      const match = cleanUrl.match(/youtube\.com\/@([^\/]+)/);
      return match ? `@${match[1]}` : (cleanUrl.split('/').pop() || url);
    }

    return url;
  } catch (e) {
    return url;
  }
}

// IST Timezone Helpers (UTC+5:30)
const IST_TIMEZONE = 'Asia/Kolkata';

/**
 * Format a date to IST timezone with a given format pattern
 * @param date - Date object or ISO string
 * @param formatStr - Format pattern (e.g., 'EEE hh:mm a', 'dd MMM yyyy')
 */
export function formatToIST(date: Date | string, formatStr: string = 'dd MMM yyyy, hh:mm a'): string {
  const d = typeof date === 'string' ? new Date(date) : date;
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIMEZONE,
    ...parseFormatString(formatStr)
  }).format(d);
}

/**
 * Get a short IST time string (e.g., "Tue 06:00 AM")
 */
export function formatShortIST(date: Date | string): string {
  const d = typeof date === 'string' ? new Date(date) : date;
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIMEZONE,
    weekday: 'short',
    hour: '2-digit',
    minute: '2-digit',
    hour12: true
  }).format(d);
}

/**
 * Get a full IST date-time string
 */
export function formatFullIST(date: Date | string): string {
  const d = typeof date === 'string' ? new Date(date) : date;
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: IST_TIMEZONE,
    day: '2-digit',
    month: 'short',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    hour12: true
  }).format(d);
}

// Helper to parse common format patterns to Intl options
function parseFormatString(formatStr: string): Intl.DateTimeFormatOptions {
  const opts: Intl.DateTimeFormatOptions = {};
  if (formatStr.includes('EEE') || formatStr.includes('ddd')) opts.weekday = 'short';
  if (formatStr.includes('EEEE')) opts.weekday = 'long';
  if (formatStr.includes('dd')) opts.day = '2-digit';
  if (formatStr.includes('MMM')) opts.month = 'short';
  if (formatStr.includes('MMMM')) opts.month = 'long';
  if (formatStr.includes('yyyy')) opts.year = 'numeric';
  if (formatStr.includes('hh') || formatStr.includes('HH')) {
    opts.hour = '2-digit';
    opts.minute = '2-digit';
  }
  if (formatStr.includes('a')) opts.hour12 = true;
  return opts;
}
