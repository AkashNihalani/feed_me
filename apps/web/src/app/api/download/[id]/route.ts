import { NextRequest, NextResponse } from 'next/server';
import { ApifyClient } from 'apify-client';
import * as XLSX from 'xlsx';
import { mapDataToExcel } from '@/lib/mappers';
import { getSupabase } from '@/lib/supabase';

// On-demand Excel generation - no storage costs!
// Fetches data from Apify and generates Excel on the fly
export async function GET(
    request: NextRequest,
    { params }: { params: Promise<{ id: string }> }
) {
    try {
        const { id: scrapeId } = await params;
        
        // 1. Get scrape record
        const { data: scrape, error } = await getSupabase(true)
            .from('scrapes')
            .select('*')
            .eq('id', scrapeId)
            .single();
        
        if (error || !scrape) {
            return NextResponse.json({ error: 'Scrape not found' }, { status: 404 });
        }
        
        if (scrape.status !== 'success') {
            return NextResponse.json({ error: 'Scrape not complete' }, { status: 400 });
        }
        
        // 2. Fetch data from Apify
        const apifyToken = process.env.APIFY_TOKEN;
        if (!apifyToken) {
            return NextResponse.json({ error: 'Server config error' }, { status: 500 });
        }
        
        const client = new ApifyClient({ token: apifyToken });
        const run = await client.run(scrape.apify_run_id).get();
        
        if (!run || !run.defaultDatasetId) {
            return NextResponse.json({ error: 'Data no longer available' }, { status: 410 });
        }
        
        const dataset = await client.dataset(run.defaultDatasetId).listItems();
        const items = dataset.items;
        
        if (!items || items.length === 0) {
            return NextResponse.json({ error: 'No data found' }, { status: 404 });
        }
        
        // 3. Generate Excel on the fly
        const excelData = mapDataToExcel(scrape.platform, items);
        const worksheet = XLSX.utils.json_to_sheet(excelData);
        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, worksheet, 'Results');
        
        // Auto-size columns
        const maxWidths: number[] = [];
        excelData.forEach((row) => {
            Object.values(row).forEach((val, idx) => {
                const len = String(val).length;
                maxWidths[idx] = Math.max(maxWidths[idx] || 10, Math.min(len, 50));
            });
        });
        worksheet['!cols'] = maxWidths.map((w) => ({ wch: w }));
        
        const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
        
        // 4. Return as downloadable file
        const fileName = `${scrape.platform}_${scrape.id}.xlsx`;
        
        return new NextResponse(buffer, {
            headers: {
                'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                'Content-Disposition': `attachment; filename="${fileName}"`,
                'Cache-Control': 'no-store, max-age=0'
            }
        });
        
    } catch (error: any) {
        console.error('[Download] Error:', error);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
