import { NextRequest, NextResponse } from 'next/server';
import { getSupabase } from '@/lib/supabase';
import { startAsyncScrape } from '@/lib/scraping';

// Free tier: 10s timeout is enough for fire-and-forget
export const maxDuration = 10;

export async function GET(request: NextRequest) {
    // 1. Security Check
    const authHeader = request.headers.get('authorization');
    if (authHeader !== `Bearer ${process.env.CRON_SECRET}`) {
        return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const now = new Date();
    console.log(`[CRON] Trigger fired at ${now.toISOString()}`);

    // 2. Fetch Active Schedules WHERE next_run_at <= now
    const { data: schedules, error } = await getSupabase(true)
        .from('scheduled_scrapes')
        .select('*')
        .eq('status', 'active')
        .lte('next_run_at', now.toISOString());

    if (error) {
        console.error('[CRON] Fetch error:', error.message);
        return NextResponse.json({ error: error.message }, { status: 500 });
    }

    if (!schedules || schedules.length === 0) {
        console.log('[CRON] No schedules due at this time.');
        return NextResponse.json({ message: 'No schedules due', checkedAt: now.toISOString() });
    }

    console.log(`[CRON] Found ${schedules.length} schedule(s) due. Triggering async scrapes...`);

    // 3. Fire-and-Forget: Trigger all scrapes in parallel, don't wait for completion
    const triggers = schedules.map(async (schedule) => {
        try {
            // 3a. Calculate NEXT run time at 6:00 AM IST (00:30 UTC) FIRST
            const nextRun = new Date();
            if (schedule.frequency === 'daily') {
                nextRun.setDate(nextRun.getDate() + 1);
            } else {
                nextRun.setDate(nextRun.getDate() + 7);
            }
            nextRun.setUTCHours(0, 30, 0, 0); // 6:00 AM IST

            // 3b. Update next_run and last_run immediately (prevents duplicate runs)
            await getSupabase(true)
                .from('scheduled_scrapes')
                .update({ 
                    next_run_at: nextRun.toISOString(),
                    last_run_at: now.toISOString()
                })
                .eq('id', schedule.id);

            // 3c. Trigger async scrape (fire-and-forget via webhook callback)
            const result = await startAsyncScrape({
                platform: schedule.platform,
                url: schedule.target_url,
                postCount: 20, // Default for automated runs
                userId: schedule.user_id,
                emailNotificationsEnabled: true,
                scheduleId: schedule.id // Link to schedule for stats tracking
            });

            console.log(`[CRON] Triggered ${schedule.id}: runId=${result.apifyRunId || 'failed'}`);
            
            return {
                id: schedule.id,
                triggered: result.success,
                runId: result.apifyRunId,
                error: result.error
            };

        } catch (err: any) {
            console.error(`[CRON] Failed to trigger ${schedule.id}:`, err.message);
            return { id: schedule.id, triggered: false, error: err.message };
        }
    });

    // Wait for all triggers (NOT the actual scrapes - those complete via webhook)
    const results = await Promise.all(triggers);

    return NextResponse.json({ 
        success: true, 
        triggered: results.length,
        results,
        note: 'Scrapes running async. Results will be processed via webhook.'
    });
}
