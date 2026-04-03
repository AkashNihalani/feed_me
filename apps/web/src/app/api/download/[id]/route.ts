import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';

export const dynamic = 'force-dynamic';

type FeedRow = {
  id: number;
  user_id: string;
  name: string;
};

type ExportRow = {
  feeder_handle: string | null;
  post_key: string;
  media_type: string | null;
  posted_at: string | null;
  caption: string | null;
  post_url: string;
  likes: number | null;
  comments: number | null;
  views: number | null;
  last_scraped_at: string | null;
  last_checkpoint: string | null;
};

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

function istDateKey(date: Date): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const values = new Map(parts.map((part) => [part.type, part.value]));
  return `${values.get('year')}-${values.get('month')}-${values.get('day')}`;
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

function xmlEscape(value: unknown): string {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

function stringCell(value: unknown): string {
  return `<Cell><Data ss:Type="String">${xmlEscape(value)}</Data></Cell>`;
}

function numberCell(value: unknown): string {
  const parsed = typeof value === 'number' ? value : Number(value);
  if (!Number.isFinite(parsed)) return stringCell('');
  return `<Cell><Data ss:Type="Number">${parsed}</Data></Cell>`;
}

function formatTimestampIst(value: string | null): string {
  if (!value) return '';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '';
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
  const values = new Map(parts.map((part) => [part.type, part.value]));
  return `${values.get('year')}-${values.get('month')}-${values.get('day')} ${values.get('hour')}:${values.get('minute')}:${values.get('second')}`;
}

function workbookXml(rows: Array<{
  posted_at_ist: string;
  feeder_handle: string;
  media_type: string;
  caption: string;
  likes: number | null;
  comments: number | null;
  views: number | null;
  last_scraped_at_ist: string;
  last_checkpoint: string;
  post_url: string;
}>): string {
  const header = [
    'Published At (IST)',
    'Handle',
    'Media Type',
    'Caption',
    'Likes',
    'Comments',
    'Views',
    'Last Scraped At (IST)',
    'Last Checkpoint',
    'Instagram Post Link',
  ]
    .map((label) => stringCell(label))
    .join('');

  const body = rows
    .map((row) => (
      `<Row>${
        [
          stringCell(row.posted_at_ist),
          stringCell(row.feeder_handle),
          stringCell(row.media_type),
          stringCell(row.caption),
          numberCell(row.likes),
          numberCell(row.comments),
          numberCell(row.views),
          stringCell(row.last_scraped_at_ist),
          stringCell(row.last_checkpoint),
          stringCell(row.post_url),
        ].join('')
      }</Row>`
    ))
    .join('');

  return `<?xml version="1.0"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook xmlns="urn:schemas-microsoft-com:office:spreadsheet"
 xmlns:o="urn:schemas-microsoft-com:office:office"
 xmlns:x="urn:schemas-microsoft-com:office:excel"
 xmlns:ss="urn:schemas-microsoft-com:office:spreadsheet">
 <Worksheet ss:Name="Posts">
  <Table>
   <Row>${header}</Row>
   ${body}
  </Table>
 </Worksheet>
</Workbook>`;
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

    if (!from || !to) {
      return NextResponse.json({ error: 'from and to must be YYYY-MM-DD dates' }, { status: 400 });
    }
    if (from > to) {
      return NextResponse.json({ error: 'from must be on or before to' }, { status: 400 });
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

    const rows = ((data || []) as ExportRow[]).map((row) => ({
      posted_at_ist: formatTimestampIst(row.posted_at),
      feeder_handle: String(row.feeder_handle || ''),
      media_type: String(row.media_type || ''),
      caption: String(row.caption || ''),
      likes: row.likes ?? null,
      comments: row.comments ?? null,
      views: row.views ?? null,
      last_scraped_at_ist: formatTimestampIst(row.last_scraped_at),
      last_checkpoint: String(row.last_checkpoint || ''),
      post_url: row.post_url,
    }));

    const xml = workbookXml(rows);
    const baseName = slugify((feedRow as FeedRow).name || 'feed_export');
    const handleSuffix = handle ? `_${slugify(handle)}` : '';
    const filename = `${baseName}${handleSuffix}_${from}_to_${to}_posts_latest.xls`;

    return new NextResponse(xml, {
      status: 200,
      headers: {
        'Content-Type': 'application/vnd.ms-excel; charset=utf-8',
        'Content-Disposition': `attachment; filename="${filename}"`,
        'Cache-Control': 'no-store',
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to generate export';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
