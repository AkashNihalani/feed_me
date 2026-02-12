import { NextRequest, NextResponse } from 'next/server';
import { processScrapeResult } from '@/lib/scraping';
import crypto from 'crypto';

export async function POST(request: NextRequest) {
    console.log('[Webhook] Received POST request');
    
    // 1. Validate Secret
    const url = new URL(request.url);
    const secret = url.searchParams.get('secret');
    const expectedSecret = process.env.CRON_SECRET;

    console.log(`[Webhook] Secret check: received=${secret ? 'present' : 'missing'}, expected=${expectedSecret ? 'configured' : 'NOT CONFIGURED'}`);

    // Using CRON_SECRET as a shared secret for now, or use a dedicated one
    if (secret !== expectedSecret) {
        console.error(`[Webhook] UNAUTHORIZED: secrets don't match`);
        return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    try {
        const body = await request.json();
        const { eventType, resource } = body;

        console.log(`[Webhook] Received Apify event: ${eventType}, runId: ${resource?.id}`);

        if (eventType === 'ACTOR.RUN.SUCCEEDED' || eventType === 'ACTOR.RUN.FAILED') {
            const runId = resource.id;
            
            console.log(`[Webhook] Processing run ${runId}...`);
            
            // 2. Process Result (Async)
            // We await it here because Vercel/Supabase Edge needs to keep alive
            await processScrapeResult(runId);
            
            console.log(`[Webhook] Successfully processed run ${runId}`);
            
            return NextResponse.json({ success: true, processed: runId });
        }

        console.log(`[Webhook] Ignoring event type: ${eventType}`);
        return NextResponse.json({ message: 'Ignored event' });

    } catch (error: any) {
        console.error('[Webhook] Error:', error.message, error.stack);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }
}
