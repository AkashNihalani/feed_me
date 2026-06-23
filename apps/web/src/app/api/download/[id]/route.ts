import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import ExcelJS from 'exceljs';
import { createClient } from '@/lib/supabase/server';
import {
  ALL_EXPORT_FIELD_IDS,
  DEFAULT_EXPORT_FIELD_IDS,
  EXPORT_FIELD_ID_SET,
  EXPORT_FIELD_LABELS,
  ExportFieldId,
} from '@/lib/feedExportConfig';

export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

type FeedRow = {
  id: number;
  user_id: string;
  name: string;
};

type ExportRow = {
  post_key?: string | null;
  feeder_handle: string | null;
  media_type: string | null;
  posted_at: string | null;
  caption: string | null;
  thumbnail_url: string | null;
  post_url: string | null;
  likes: number | null;
  comments: number | null;
  views: number | null;
  percentile_performance: number | null;
  views_percentile: number | null;
  likes_percentile: number | null;
  comments_percentile: number | null;
  delta_from_d1: number | null;
};

type TrackingCheckpoint = 'd1' | 'd3' | 'd7' | 'd21';

type CheckpointMetric = {
  views: number | null;
  likes: number | null;
  comments: number | null;
  overallPercentile: number | null;
  viewsPercentile: number | null;
  likesPercentile: number | null;
  commentsPercentile: number | null;
  deltaFromD1: number | null;
};

type PostMetricRow = {
  post_key: string | null;
  checkpoint: string | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
  percentile_performance: number | string | null;
  percentile_performance_exact?: number | string | null;
  views_percentile: number | null;
  likes_percentile: number | null;
  comments_percentile: number | null;
  delta_from_d1: number | null;
};

type PostThumbnailRow = {
  post_key: string | null;
  thumbnail_url: string | null;
};

type WorkbookRow = {
  handle: string;
  publishedAtIst: Date | null;
  publishedDateKeyIst: string | null;
  mediaType: string;
  caption: string;
  thumbnailUrl: string;
  postUrl: string;
  likes: number | null;
  comments: number | null;
  views: number | null;
  overallPercentile: number | null;
  viewsPercentile: number | null;
  likesPercentile: number | null;
  commentsPercentile: number | null;
  deltaFromD1: number | null;
  checkpoints: Partial<Record<TrackingCheckpoint, CheckpointMetric>>;
};

type ExportSheetId = 'summary' | 'posts';

const TRACKING_CHECKPOINTS = ['d1', 'd3', 'd7', 'd21'] as const;
const CHECKPOINT_RANK: Record<TrackingCheckpoint, number> = {
  d1: 1,
  d3: 2,
  d7: 3,
  d21: 4,
};
const POST_KEY_CHUNK_SIZE = 250;
const ALLOWED_SHEETS = new Set<ExportSheetId>(['summary', 'posts']);
const DEFAULT_SHEETS = new Set<ExportSheetId>(['summary', 'posts']);
const ROSE = 'FFE11D48';
const ROSE_DARK = 'FFBE123C';
const TEXT_DARK = 'FF111827';
const TEXT_MUTED = 'FF6B7280';
const BORDER_LIGHT = 'FFE5E7EB';

function adminClient() {
  const url = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRole = process.env.SUPABASE_SERVICE_ROLE_KEY;
  if (!url || !serviceRole) {
    throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
  }
  return createSupabaseClient(url, serviceRole, {
    auth: { autoRefreshToken: false, persistSession: false },
  });
}

function normalizeHandle(raw: string | null): string | null {
  const cleaned = String(raw || '').trim().replace(/^@+/, '').toLowerCase();
  return cleaned || null;
}

function slugify(value: string): string {
  return value
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '') || 'feed_export';
}

function parseDateParam(value: string | null): string | null {
  const cleaned = String(value || '').trim();
  if (!cleaned) return null;
  return /^\d{4}-\d{2}-\d{2}$/.test(cleaned) ? cleaned : null;
}

function utcDateKey(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function dateKeyInTimeZone(date: Date, timeZone: string): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const values = new Map(parts.map((part) => [part.type, part.value]));
  return `${values.get('year')}-${values.get('month')}-${values.get('day')}`;
}

function istDateKey(date: Date): string {
  return dateKeyInTimeZone(date, 'Asia/Kolkata');
}

function shiftIsoDate(isoDate: string, days: number): string {
  const [year, month, day] = isoDate.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  date.setUTCDate(date.getUTCDate() + days);
  return utcDateKey(date);
}

function defaultExportRange(): { from: string; to: string } {
  const to = istDateKey(new Date());
  return { from: shiftIsoDate(to, -89), to };
}

function parseSelectedFields(raw: string | null): { fields: ExportFieldId[]; error: string | null } {
  if (!raw) return { fields: [...DEFAULT_EXPORT_FIELD_IDS], error: null };
  const ids = raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  if (ids.length === 0) return { fields: [...DEFAULT_EXPORT_FIELD_IDS], error: null };

  const fields: ExportFieldId[] = [];
  const seen = new Set<string>();
  for (const id of ids) {
    if (!EXPORT_FIELD_ID_SET.has(id as ExportFieldId)) {
      return { fields: [], error: `Unknown export field: ${id}` };
    }
    if (!seen.has(id)) {
      fields.push(id as ExportFieldId);
      seen.add(id);
    }
  }
  return { fields, error: null };
}

function parseSelectedSheets(raw: string | null): { sheets: Set<ExportSheetId>; error: string | null } {
  if (!raw) return { sheets: new Set(DEFAULT_SHEETS), error: null };
  const ids = raw
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  if (ids.length === 0) return { sheets: new Set(DEFAULT_SHEETS), error: null };

  const sheets = new Set<ExportSheetId>();
  for (const id of ids) {
    if (!ALLOWED_SHEETS.has(id as ExportSheetId)) {
      return { sheets: new Set(), error: `Unknown export sheet: ${id}` };
    }
    sheets.add(id as ExportSheetId);
  }
  sheets.add('posts');
  return { sheets, error: null };
}

function nullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function externalUrl(value: unknown): string {
  const raw = typeof value === 'string' ? value.trim() : '';
  if (!raw) return '';
  try {
    const url = new URL(raw);
    if (url.protocol !== 'http:' && url.protocol !== 'https:') return '';
    if (url.hostname === 'localhost' || url.hostname === '127.0.0.1') return '';
    return url.toString();
  } catch {
    return '';
  }
}

function titleCase(value: string): string {
  const clean = value.trim().replace(/[_-]+/g, ' ').toLowerCase();
  if (!clean) return '';
  return clean.replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizeCheckpoint(value: unknown): TrackingCheckpoint | null {
  const normalized = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return normalized === 'd1' || normalized === 'd3' || normalized === 'd7' || normalized === 'd21'
    ? normalized
    : null;
}

function istParts(value: string): Map<Intl.DateTimeFormatPartTypes, string> | null {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(date);
  return new Map(parts.map((part) => [part.type, part.value]));
}

function excelDateFromTimestampIst(value: string | null): Date | null {
  if (!value) return null;
  const values = istParts(value);
  if (!values) return null;
  const year = Number(values.get('year'));
  const month = Number(values.get('month'));
  const day = Number(values.get('day'));
  const hour = Number(values.get('hour'));
  const minute = Number(values.get('minute'));
  const second = Number(values.get('second'));
  if (![year, month, day, hour, minute, second].every(Number.isFinite)) return null;
  return new Date(Date.UTC(year, month - 1, day, hour, minute, second));
}

function istDateKeyFromTimestamp(value: string | null): string | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return istDateKey(date);
}

function weekStartIso(isoDate: string): string {
  const [year, month, day] = isoDate.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  const dayOfWeek = date.getUTCDay();
  const diff = dayOfWeek === 0 ? -6 : 1 - dayOfWeek;
  date.setUTCDate(date.getUTCDate() + diff);
  return utcDateKey(date);
}

function formatDisplayDate(isoDate: string): string {
  const [year, month, day] = isoDate.split('-').map(Number);
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  }).format(new Date(Date.UTC(year, month - 1, day)));
}

function formatGeneratedAt(): string {
  const now = new Date();
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).formatToParts(now);
  const values = new Map(parts.map((part) => [part.type, part.value]));
  return `${values.get('year')}-${values.get('month')}-${values.get('day')} ${values.get('hour')}:${values.get('minute')} IST`;
}

async function fetchMetricRowsForPostKeys(
  sb: ReturnType<typeof adminClient>,
  postKeys: string[],
): Promise<PostMetricRow[]> {
  if (postKeys.length === 0) return [];

  const rows: PostMetricRow[] = [];
  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('post_metrics')
      .select('post_key,checkpoint,views,likes,comments,percentile_performance,percentile_performance_exact,views_percentile,likes_percentile,comments_percentile,delta_from_d1')
      .in('post_key', chunk)
      .in('checkpoint', [...TRACKING_CHECKPOINTS]);

    if (error) throw error;
    rows.push(...((data || []) as PostMetricRow[]));
  }

  return rows;
}

async function fetchThumbnailsForPostKeys(
  sb: ReturnType<typeof adminClient>,
  postKeys: string[],
): Promise<Map<string, string>> {
  const thumbnailByPostKey = new Map<string, string>();
  if (postKeys.length === 0) return thumbnailByPostKey;

  for (let start = 0; start < postKeys.length; start += POST_KEY_CHUNK_SIZE) {
    const chunk = postKeys.slice(start, start + POST_KEY_CHUNK_SIZE);
    const { data, error } = await sb
      .from('posts')
      .select('post_key,thumbnail_url')
      .in('post_key', chunk);

    if (error) throw error;
    for (const row of (data || []) as PostThumbnailRow[]) {
      const postKey = typeof row.post_key === 'string' ? row.post_key : '';
      const thumbnailUrl = externalUrl(row.thumbnail_url);
      if (postKey && thumbnailUrl) thumbnailByPostKey.set(postKey, thumbnailUrl);
    }
  }

  return thumbnailByPostKey;
}

function metricFromRow(row: PostMetricRow): CheckpointMetric {
  return {
    views: nullableNumber(row.views),
    likes: nullableNumber(row.likes),
    comments: nullableNumber(row.comments),
    overallPercentile: nullableNumber(row.percentile_performance_exact) ?? nullableNumber(row.percentile_performance),
    viewsPercentile: nullableNumber(row.views_percentile),
    likesPercentile: nullableNumber(row.likes_percentile),
    commentsPercentile: nullableNumber(row.comments_percentile),
    deltaFromD1: nullableNumber(row.delta_from_d1),
  };
}

function buildMetricsByPostKey(metricRows: PostMetricRow[]) {
  const metricsByPostKey = new Map<string, Partial<Record<TrackingCheckpoint, CheckpointMetric>>>();
  for (const row of metricRows) {
    const postKey = typeof row.post_key === 'string' ? row.post_key : '';
    const checkpoint = normalizeCheckpoint(row.checkpoint);
    if (!postKey || !checkpoint) continue;
    const bucket = metricsByPostKey.get(postKey) || {};
    bucket[checkpoint] = metricFromRow(row);
    metricsByPostKey.set(postKey, bucket);
  }
  return metricsByPostKey;
}

function latestCheckpointMetric(
  checkpoints: Partial<Record<TrackingCheckpoint, CheckpointMetric>>,
): CheckpointMetric | null {
  let latest: { checkpoint: TrackingCheckpoint; metric: CheckpointMetric } | null = null;
  for (const checkpoint of TRACKING_CHECKPOINTS) {
    const metric = checkpoints[checkpoint];
    if (!metric) continue;
    if (!latest || CHECKPOINT_RANK[checkpoint] > CHECKPOINT_RANK[latest.checkpoint]) {
      latest = { checkpoint, metric };
    }
  }
  return latest?.metric ?? null;
}

function mapWorkbookRows(
  rows: ExportRow[],
  metricsByPostKey: Map<string, Partial<Record<TrackingCheckpoint, CheckpointMetric>>>,
  thumbnailByPostKey: Map<string, string>,
): WorkbookRow[] {
  return rows.map((row) => {
    const postKey = typeof row.post_key === 'string' ? row.post_key : '';
    const checkpoints = postKey ? metricsByPostKey.get(postKey) || {} : {};
    const latestMetric = latestCheckpointMetric(checkpoints);
    return {
    handle: String(row.feeder_handle || ''),
    publishedAtIst: excelDateFromTimestampIst(row.posted_at),
    publishedDateKeyIst: istDateKeyFromTimestamp(row.posted_at),
    mediaType: titleCase(String(row.media_type || '')),
    caption: String(row.caption || ''),
    thumbnailUrl: externalUrl(row.thumbnail_url) || (postKey ? thumbnailByPostKey.get(postKey) || '' : ''),
    postUrl: externalUrl(row.post_url),
    likes: latestMetric?.likes ?? nullableNumber(row.likes),
    comments: latestMetric?.comments ?? nullableNumber(row.comments),
    views: latestMetric?.views ?? nullableNumber(row.views),
    overallPercentile: latestMetric?.overallPercentile ?? nullableNumber(row.percentile_performance),
    viewsPercentile: latestMetric?.viewsPercentile ?? nullableNumber(row.views_percentile),
    likesPercentile: latestMetric?.likesPercentile ?? nullableNumber(row.likes_percentile),
    commentsPercentile: latestMetric?.commentsPercentile ?? nullableNumber(row.comments_percentile),
    deltaFromD1: latestMetric?.deltaFromD1 ?? nullableNumber(row.delta_from_d1),
    checkpoints,
    };
  });
}

function asLink(label: string, url: string): ExcelJS.CellValue {
  return url ? { text: label, hyperlink: url, tooltip: url } : '';
}

function checkpointFieldValue(row: WorkbookRow, field: ExportFieldId): ExcelJS.CellValue | undefined {
  const match = /^(d1|d3|d7|d21)_(views|likes|comments|overall_percentile|views_percentile|likes_percentile|comments_percentile|delta_from_d1)$/.exec(field);
  if (!match) return undefined;
  const checkpoint = match[1] as TrackingCheckpoint;
  const metricKey = match[2];
  const metric = row.checkpoints[checkpoint];
  if (!metric) return null;
  switch (metricKey) {
    case 'views':
      return metric.views;
    case 'likes':
      return metric.likes;
    case 'comments':
      return metric.comments;
    case 'overall_percentile':
      return metric.overallPercentile;
    case 'views_percentile':
      return metric.viewsPercentile;
    case 'likes_percentile':
      return metric.likesPercentile;
    case 'comments_percentile':
      return metric.commentsPercentile;
    case 'delta_from_d1':
      return metric.deltaFromD1;
    default:
      return null;
  }
}

function fieldValue(row: WorkbookRow, field: ExportFieldId): ExcelJS.CellValue {
  const checkpointValue = checkpointFieldValue(row, field);
  if (checkpointValue !== undefined) return checkpointValue;

  switch (field) {
    case 'handle':
      return row.handle ? `@${row.handle}` : '';
    case 'published_at_ist':
      return row.publishedAtIst;
    case 'media_type':
      return row.mediaType;
    case 'post_link':
      return asLink('Open post', row.postUrl);
    case 'caption':
      return row.caption;
    case 'thumbnail_link':
      return asLink('Thumbnail', row.thumbnailUrl);
    case 'views':
      return row.views;
    case 'likes':
      return row.likes;
    case 'comments':
      return row.comments;
    case 'overall_percentile':
      return row.overallPercentile;
    case 'views_percentile':
      return row.viewsPercentile;
    case 'likes_percentile':
      return row.likesPercentile;
    case 'comments_percentile':
      return row.commentsPercentile;
    case 'delta_from_d1':
      return row.deltaFromD1;
    default:
      return '';
  }
}

function columnWidth(field: ExportFieldId): number {
  if (field.includes('percentile')) return 18;
  if (field.includes('delta_from_d1')) return 16;
  if (field.startsWith('d1_') || field.startsWith('d3_') || field.startsWith('d7_') || field.startsWith('d21_')) return 14;
  switch (field) {
    case 'caption':
      return 58;
    case 'post_link':
    case 'thumbnail_link':
      return 20;
    case 'published_at_ist':
      return 21;
    case 'media_type':
      return 15;
    case 'handle':
      return 18;
    case 'overall_percentile':
    case 'views_percentile':
    case 'likes_percentile':
    case 'comments_percentile':
      return 18;
    default:
      return 14;
  }
}

function isNumberField(field: ExportFieldId): boolean {
  return !['handle', 'published_at_ist', 'media_type', 'post_link', 'caption', 'thumbnail_link'].includes(field);
}

function isPercentileField(field: ExportFieldId): boolean {
  return field.includes('percentile');
}

function styleHeaderRow(row: ExcelJS.Row) {
  row.height = 28;
  row.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 11 };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF111111' } };
    cell.alignment = { vertical: 'middle', horizontal: 'center' };
    cell.border = {
      bottom: { style: 'medium', color: { argb: ROSE } },
    };
  });
}

function stylePostsSheet(sheet: ExcelJS.Worksheet, fields: ExportFieldId[], rowCount: number) {
  sheet.views = [{ state: 'frozen', ySplit: 4, activeCell: 'A5', showGridLines: false }];
  sheet.properties.defaultRowHeight = 18;
  styleHeaderRow(sheet.getRow(4));

  fields.forEach((field, index) => {
    const column = sheet.getColumn(index + 1);
    column.width = columnWidth(field);
    column.eachCell({ includeEmpty: false }, (cell, rowNumber) => {
      if (rowNumber <= 4) return;
      cell.alignment = {
        vertical: 'middle',
        horizontal: isNumberField(field) ? 'right' : 'left',
        wrapText: false,
      };
      cell.border = {
        bottom: { style: 'thin', color: { argb: BORDER_LIGHT } },
      };
      if (field === 'published_at_ist') cell.numFmt = 'yyyy-mm-dd hh:mm';
      if (['views', 'likes', 'comments', 'delta_from_d1'].includes(field)) cell.numFmt = '#,##0';
      if (isPercentileField(field)) cell.numFmt = '0.0';
      if (field === 'post_link' || field === 'thumbnail_link') {
        cell.font = { color: { argb: ROSE_DARK }, underline: true };
      }
    });
  });

  for (let rowNumber = 5; rowNumber <= rowCount + 4; rowNumber += 1) {
    const row = sheet.getRow(rowNumber);
    row.height = 22;
    row.eachCell((cell) => {
      cell.fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: rowNumber % 2 === 0 ? 'FFFFFFFF' : 'FFFAFAFA' },
      };
      if (typeof cell.value !== 'object' || !cell.value || !('hyperlink' in cell.value)) {
        cell.font = { color: { argb: TEXT_DARK }, size: 10 };
      }
    });
  }
}

function columnNumberToName(value: number): string {
  let current = value;
  let name = '';
  while (current > 0) {
    const remainder = (current - 1) % 26;
    name = String.fromCharCode(65 + remainder) + name;
    current = Math.floor((current - 1) / 26);
  }
  return name;
}

function addPostsSheet(
  workbook: ExcelJS.Workbook,
  rows: WorkbookRow[],
  fields: ExportFieldId[],
  options: { feedName: string; scopeLabel: string; from: string; to: string },
) {
  const sheet = workbook.addWorksheet('Posts', {
    pageSetup: { orientation: 'landscape', fitToPage: true, fitToWidth: 1, fitToHeight: 0 },
    views: [{ state: 'frozen', ySplit: 4, showGridLines: false }],
  });

  const lastColumn = columnNumberToName(Math.max(fields.length, 1));
  sheet.mergeCells(`A1:${lastColumn}2`);
  const title = sheet.getCell('A1');
  title.value = 'POSTS';
  title.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 30 };
  title.alignment = { vertical: 'middle', horizontal: 'left' };
  title.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF111111' } };
  sheet.getRow(1).height = 34;
  sheet.getRow(2).height = 18;

  sheet.mergeCells(`A3:${lastColumn}3`);
  const meta = sheet.getCell('A3');
  meta.value = `${options.feedName.toUpperCase()} | ${options.scopeLabel.toUpperCase()} | ${formatDisplayDate(options.from)} TO ${formatDisplayDate(options.to)}`;
  meta.font = { bold: true, color: { argb: TEXT_MUTED }, size: 10 };
  meta.alignment = { vertical: 'middle', horizontal: 'left' };
  meta.border = { bottom: { style: 'medium', color: { argb: ROSE } } };

  fields.forEach((field, index) => {
    sheet.getCell(4, index + 1).value = EXPORT_FIELD_LABELS[field];
  });
  rows.forEach((row, rowIndex) => {
    fields.forEach((field, fieldIndex) => {
      sheet.getCell(rowIndex + 5, fieldIndex + 1).value = fieldValue(row, field);
    });
  });

  sheet.autoFilter = {
    from: { row: 4, column: 1 },
    to: { row: Math.max(4, rows.length + 4), column: fields.length },
  };

  stylePostsSheet(sheet, fields, rows.length);
  return sheet;
}

function columnNameToNumber(value: string): number {
  return value.split('').reduce((sum, char) => sum * 26 + char.charCodeAt(0) - 64, 0);
}

function cellAddress(address: string): { row: number; col: number } {
  const match = /^([A-Z]+)(\d+)$/.exec(address);
  if (!match) throw new Error(`Invalid cell address: ${address}`);
  return { col: columnNameToNumber(match[1]), row: Number(match[2]) };
}

function rangeBounds(ref: string): { startRow: number; startCol: number; endRow: number; endCol: number } {
  const [start, end = start] = ref.split(':');
  const startCell = cellAddress(start);
  const endCell = cellAddress(end);
  return {
    startRow: Math.min(startCell.row, endCell.row),
    startCol: Math.min(startCell.col, endCell.col),
    endRow: Math.max(startCell.row, endCell.row),
    endCol: Math.max(startCell.col, endCell.col),
  };
}

function writeValues(sheet: ExcelJS.Worksheet, startRef: string, values: ExcelJS.CellValue[][]) {
  const start = cellAddress(startRef);
  values.forEach((row, rowIndex) => {
    row.forEach((value, colIndex) => {
      sheet.getCell(start.row + rowIndex, start.col + colIndex).value = value;
    });
  });
}

function forEachCellInRange(sheet: ExcelJS.Worksheet, ref: string, callback: (cell: ExcelJS.Cell) => void) {
  const bounds = rangeBounds(ref);
  for (let row = bounds.startRow; row <= bounds.endRow; row += 1) {
    for (let col = bounds.startCol; col <= bounds.endCol; col += 1) {
      callback(sheet.getCell(row, col));
    }
  }
}

function numberOrZero(value: number | null): number {
  return Number.isFinite(value) ? Number(value) : 0;
}

function average(values: Array<number | null>): number | null {
  const usable = values.filter((value): value is number => typeof value === 'number' && Number.isFinite(value));
  if (usable.length === 0) return null;
  return usable.reduce((sum, value) => sum + value, 0) / usable.length;
}

function bestPost(rows: WorkbookRow[]): WorkbookRow | null {
  if (rows.length === 0) return null;
  const withPercentiles = rows.filter((row) => row.overallPercentile != null);
  if (withPercentiles.length > 0) {
    return withPercentiles.reduce((best, row) => (
      (row.overallPercentile ?? 101) < (best.overallPercentile ?? 101) ? row : best
    ), withPercentiles[0]);
  }
  return rows.reduce((best, row) => (
    numberOrZero(row.views) > numberOrZero(best.views) ? row : best
  ), rows[0]);
}

function stylePanel(sheet: ExcelJS.Worksheet, ref: string) {
  forEachCellInRange(sheet, ref, (cell) => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FFFFFFFF' } };
    cell.border = {
      top: { style: 'thin', color: { argb: BORDER_LIGHT } },
      left: { style: 'thin', color: { argb: BORDER_LIGHT } },
      bottom: { style: 'thin', color: { argb: BORDER_LIGHT } },
      right: { style: 'thin', color: { argb: BORDER_LIGHT } },
    };
  });
}

function addMetricCard(sheet: ExcelJS.Worksheet, ref: string, label: string, value: string | number) {
  const [start, end] = ref.split(':');
  const bounds = rangeBounds(ref);
  const valueRef = `${start}:${columnNumberToName(bounds.endCol)}${bounds.endRow - 1}`;
  const labelRef = `${columnNumberToName(bounds.startCol)}${bounds.endRow}:${end}`;
  sheet.mergeCells(valueRef);
  sheet.mergeCells(labelRef);

  stylePanel(sheet, ref);

  const valueCell = sheet.getCell(start);
  valueCell.value = value;
  valueCell.font = { bold: true, color: { argb: TEXT_DARK }, size: 22 };
  valueCell.alignment = { vertical: 'bottom', horizontal: 'center' };

  const labelCell = sheet.getCell(columnNumberToName(bounds.startCol) + bounds.endRow);
  labelCell.value = label.toUpperCase();
  labelCell.font = { bold: true, color: { argb: TEXT_MUTED }, size: 9 };
  labelCell.alignment = { vertical: 'top', horizontal: 'center' };
}

function addSectionHeader(sheet: ExcelJS.Worksheet, ref: string, label: string) {
  sheet.mergeCells(ref);
  const cell = sheet.getCell(ref.split(':')[0]);
  cell.value = label;
  cell.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 11 };
  cell.alignment = { vertical: 'middle', horizontal: 'left' };
  forEachCellInRange(sheet, ref, (sectionCell) => {
    sectionCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF111111' } };
    sectionCell.border = { bottom: { style: 'medium', color: { argb: ROSE } } };
  });
}

function fillRange(sheet: ExcelJS.Worksheet, ref: string, color: string) {
  forEachCellInRange(sheet, ref, (cell) => {
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: color } };
  });
}

function metricExists(metric: CheckpointMetric | undefined): boolean {
  return Boolean(metric && (
    metric.views != null
    || metric.likes != null
    || metric.comments != null
    || metric.overallPercentile != null
  ));
}

function addSummarySheet(
  workbook: ExcelJS.Workbook,
  rows: WorkbookRow[],
  options: { feedName: string; scopeLabel: string; from: string; to: string },
) {
  const sheet = workbook.addWorksheet('Summary', {
    views: [{ showGridLines: false }],
    pageSetup: { orientation: 'landscape', fitToPage: true, fitToWidth: 1, fitToHeight: 0 },
  });
  sheet.columns = [
    { width: 3 },
    { width: 15 },
    { width: 15 },
    { width: 15 },
    { width: 4 },
    { width: 15 },
    { width: 15 },
    { width: 15 },
    { width: 4 },
    { width: 15 },
    { width: 15 },
    { width: 15 },
  ];

  fillRange(sheet, 'A1:A5', ROSE);
  fillRange(sheet, 'B1:L5', 'FF111111');
  sheet.mergeCells('B1:L3');
  const title = sheet.getCell('B1');
  title.value = `${options.feedName.toUpperCase()} EXPORT`;
  title.font = { bold: true, color: { argb: 'FFFFFFFF' }, size: 34 };
  title.alignment = { vertical: 'middle', horizontal: 'left' };
  sheet.getRow(1).height = 32;
  sheet.getRow(2).height = 28;
  sheet.getRow(3).height = 24;

  sheet.mergeCells('B4:L5');
  const subtitle = sheet.getCell('B4');
  subtitle.value = `${options.scopeLabel.toUpperCase()}  /  ${formatDisplayDate(options.from).toUpperCase()} - ${formatDisplayDate(options.to).toUpperCase()}  /  GENERATED ${formatGeneratedAt().toUpperCase()}`;
  subtitle.font = { bold: true, color: { argb: 'FFB8B8B8' }, size: 10 };
  subtitle.alignment = { vertical: 'middle', horizontal: 'left' };

  const totalViews = rows.reduce((sum, row) => sum + numberOrZero(row.views), 0);
  const totalLikes = rows.reduce((sum, row) => sum + numberOrZero(row.likes), 0);
  const totalComments = rows.reduce((sum, row) => sum + numberOrZero(row.comments), 0);
  const avgPercentile = average(rows.map((row) => row.overallPercentile));
  const postsWithMetrics = rows.filter((row) => (
    row.views != null || row.likes != null || row.comments != null || row.overallPercentile != null
  )).length;

  addMetricCard(sheet, 'B7:D11', 'Posts', rows.length);
  addMetricCard(sheet, 'F7:H11', 'Measured', postsWithMetrics);
  addMetricCard(sheet, 'J7:L11', 'Views', totalViews.toLocaleString('en-US'));
  addMetricCard(sheet, 'B13:D17', 'Likes', totalLikes.toLocaleString('en-US'));
  addMetricCard(sheet, 'F13:H17', 'Comments', totalComments.toLocaleString('en-US'));
  addMetricCard(sheet, 'J13:L17', 'Avg Overall %', avgPercentile == null ? '--' : avgPercentile.toFixed(1));

  addSectionHeader(sheet, 'B20:D20', 'MEDIA MIX');
  const mediaCounts = new Map<string, number>();
  for (const row of rows) {
    const key = row.mediaType || 'Unknown';
    mediaCounts.set(key, (mediaCounts.get(key) || 0) + 1);
  }
  const mediaRows = [...mediaCounts.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([mediaType, count]) => [mediaType, count, rows.length > 0 ? count / rows.length : 0]);
  writeValues(sheet, 'B21', [['Type', 'Posts', 'Share']]);
  if (mediaRows.length > 0) writeValues(sheet, 'B22', mediaRows);
  styleHeaderRow(sheet.getRow(21));
  const mediaEnd = Math.max(22, 21 + mediaRows.length);
  stylePanel(sheet, `B21:D${mediaEnd}`);
  sheet.getColumn(4).numFmt = '0.0%';

  addSectionHeader(sheet, 'F20:L20', 'WEEKLY POSTING');
  const weekCounts = new Map<string, number>();
  for (const row of rows) {
    if (!row.publishedDateKeyIst) continue;
    const week = weekStartIso(row.publishedDateKeyIst);
    weekCounts.set(week, (weekCounts.get(week) || 0) + 1);
  }
  const weekRows = [...weekCounts.entries()]
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([week, count]) => [formatDisplayDate(week), count, '', '', '', '', '']);
  writeValues(sheet, 'F21', [['Week Starting', 'Posts', '', '', '', '', '']]);
  if (weekRows.length > 0) writeValues(sheet, 'F22', weekRows);
  styleHeaderRow(sheet.getRow(21));
  const weekEnd = Math.max(22, 21 + weekRows.length);
  stylePanel(sheet, `F21:L${weekEnd}`);

  addSectionHeader(sheet, 'B31:L31', 'CHECKPOINT COVERAGE');
  const checkpointRows = TRACKING_CHECKPOINTS.map((checkpoint) => {
    const count = rows.filter((row) => metricExists(row.checkpoints[checkpoint])).length;
    const avgCheckpointPercentile = average(rows.map((row) => row.checkpoints[checkpoint]?.overallPercentile ?? null));
    const checkpointViews = rows.reduce((sum, row) => sum + numberOrZero(row.checkpoints[checkpoint]?.views ?? null), 0);
    return [
      checkpoint.toUpperCase(),
      count,
      avgCheckpointPercentile == null ? null : Number(avgCheckpointPercentile.toFixed(1)),
      checkpointViews,
      '',
      '',
      '',
      '',
      '',
      '',
      '',
    ];
  });
  writeValues(sheet, 'B32', [['Checkpoint', 'Posts', 'Avg Overall %', 'Views', '', '', '', '', '', '', '']]);
  writeValues(sheet, 'B33', checkpointRows);
  styleHeaderRow(sheet.getRow(32));
  stylePanel(sheet, 'B32:L36');
  sheet.getColumn(5).numFmt = '#,##0';

  const top = bestPost(rows);
  addSectionHeader(sheet, 'B39:L39', 'TOP POST');
  sheet.mergeCells('B40:L42');
  const topCell = sheet.getCell('B40');
  if (top) {
    const pieces = [
      top.handle ? `@${top.handle}` : null,
      top.mediaType || null,
      top.publishedDateKeyIst ? formatDisplayDate(top.publishedDateKeyIst) : null,
      top.overallPercentile != null ? `Top ${top.overallPercentile.toFixed(1)}%` : null,
    ].filter(Boolean);
    topCell.value = top.postUrl
      ? { text: `${pieces.join(' | ')} | Open post`, hyperlink: top.postUrl, tooltip: top.postUrl }
      : pieces.join(' | ');
    topCell.font = { bold: true, color: { argb: ROSE_DARK }, underline: Boolean(top.postUrl), size: 13 };
  } else {
    topCell.value = 'No posts found in this export window.';
    topCell.font = { bold: true, color: { argb: TEXT_MUTED }, size: 13 };
  }
  topCell.alignment = { vertical: 'middle', horizontal: 'left', wrapText: true };
  stylePanel(sheet, 'B40:L42');

  return sheet;
}

async function buildWorkbook({
  rows,
  fields,
  sheets,
  feedName,
  scopeLabel,
  from,
  to,
}: {
  rows: WorkbookRow[];
  fields: ExportFieldId[];
  sheets: Set<ExportSheetId>;
  feedName: string;
  scopeLabel: string;
  from: string;
  to: string;
}) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'FeedMe';
  workbook.lastModifiedBy = 'FeedMe';
  workbook.created = new Date();
  workbook.modified = new Date();
  workbook.calcProperties.fullCalcOnLoad = true;

  const selectedFields = fields.length > 0 ? fields : [...ALL_EXPORT_FIELD_IDS];
  if (sheets.has('summary')) {
    addSummarySheet(workbook, rows, { feedName, scopeLabel, from, to });
  }
  addPostsSheet(workbook, rows, selectedFields, { feedName, scopeLabel, from, to });
  return workbook;
}

export async function GET(request: NextRequest, context: { params: Promise<{ id: string }> }) {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const params = await context.params;
    const feedId = Number(params.id || 0);
    if (!feedId) {
      return NextResponse.json({ error: 'Invalid feed id' }, { status: 400 });
    }

    const handle = normalizeHandle(request.nextUrl.searchParams.get('handle'));
    const rangeDefaults = defaultExportRange();
    const requestedFrom = request.nextUrl.searchParams.get('from');
    const requestedTo = request.nextUrl.searchParams.get('to');
    const from = requestedFrom ? parseDateParam(requestedFrom) : rangeDefaults.from;
    const to = requestedTo ? parseDateParam(requestedTo) : rangeDefaults.to;
    const { fields, error: fieldsError } = parseSelectedFields(request.nextUrl.searchParams.get('fields'));
    const { sheets, error: sheetsError } = parseSelectedSheets(request.nextUrl.searchParams.get('sheets'));

    if (!from || !to) {
      return NextResponse.json({ error: 'from and to must be YYYY-MM-DD dates' }, { status: 400 });
    }
    if (from > to) {
      return NextResponse.json({ error: 'from must be on or before to' }, { status: 400 });
    }
    if (fieldsError) {
      return NextResponse.json({ error: fieldsError }, { status: 400 });
    }
    if (sheetsError) {
      return NextResponse.json({ error: sheetsError }, { status: 400 });
    }

    const sb = adminClient();
    const { data: feedRow, error: feedError } = await sb
      .from('feeds')
      .select('id,user_id,name')
      .eq('id', feedId)
      .eq('user_id', user.id)
      .single();

    if (feedError || !feedRow) {
      return NextResponse.json({ error: 'Feed not found' }, { status: 404 });
    }

    const { data, error } = await sb.rpc('fn_feed_export_latest', {
      p_feed_id: feedId,
      p_handle: handle,
      p_start_date: from,
      p_end_date: to,
    });
    if (error) throw error;

    const rawRows = (data || []) as ExportRow[];
    const postKeys = rawRows
      .map((row) => (typeof row.post_key === 'string' ? row.post_key : ''))
      .filter((value): value is string => Boolean(value));
    const uniquePostKeys = [...new Set(postKeys)];
    const [metricRows, thumbnailByPostKey] = await Promise.all([
      fetchMetricRowsForPostKeys(sb, uniquePostKeys),
      fetchThumbnailsForPostKeys(sb, uniquePostKeys),
    ]);
    const metricsByPostKey = buildMetricsByPostKey(metricRows);
    const rows = mapWorkbookRows(rawRows, metricsByPostKey, thumbnailByPostKey);
    const feedName = String((feedRow as FeedRow).name || 'Feed Export');
    const scopeLabel = handle ? `@${handle}` : 'Full Feed';
    const workbook = await buildWorkbook({
      rows,
      fields,
      sheets,
      feedName,
      scopeLabel,
      from,
      to,
    });
    const buffer = await workbook.xlsx.writeBuffer();
    const baseName = slugify(feedName || 'feed_export');
    const handleSuffix = handle ? `_${slugify(handle)}` : '';
    const filename = `${baseName}${handleSuffix}_${from}_to_${to}_posts.xlsx`;

    return new NextResponse(buffer, {
      status: 200,
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="${filename}"`,
        'Cache-Control': 'no-store',
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to generate export';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
