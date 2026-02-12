import { ApifyClient } from 'apify-client';
import * as XLSX from 'xlsx';
import { mapDataToExcel } from '@/lib/mappers';
import { getSupabase, PLATFORM_RATES } from '@/lib/supabase';
import { sendScrapeResultEmail } from '@/lib/emailService';
import { extractHandle } from '@/lib/utils';

// Validate that scraped data matches the requested target
function validateScrapedData(
    platform: string, 
    requestedUrl: string, 
    items: Record<string, unknown>[]
): { valid: boolean; matchedCount: number; error?: string } {
    if (!items || items.length === 0) {
        return { valid: false, matchedCount: 0, error: 'No data returned' };
    }

    // Extract expected handle from URL (lowercase for comparison)
    const expectedHandle = extractHandle(requestedUrl, platform).toLowerCase().replace('@', '');
    
    // Define field names where handle/username appears for each platform
    const handleFields: Record<string, string[]> = {
        instagram: ['ownerUsername', 'username', 'owner'],
        youtube: ['channelName', 'channelTitle', 'authorName'],
        linkedin: ['authorName', 'author', 'profileUrl'],
        x: ['author', 'username', 'user_screen_name']
    };

    const fieldsToCheck = handleFields[platform] || [];
    let matchedCount = 0;

    for (const item of items) {
        let itemMatches = false;
        
        for (const field of fieldsToCheck) {
            const value = item[field];
            if (typeof value === 'string') {
                const normalizedValue = value.toLowerCase().replace('@', '');
                if (normalizedValue.includes(expectedHandle) || expectedHandle.includes(normalizedValue)) {
                    itemMatches = true;
                    break;
                }
            }
        }
        
        if (itemMatches) matchedCount++;
    }

    // At least 50% of items should match the expected handle
    const matchRatio = matchedCount / items.length;
    
    if (matchRatio < 0.5) {
        return { 
            valid: false, 
            matchedCount,
            error: `Data validation failed: Only ${matchedCount}/${items.length} items matched the requested account "${expectedHandle}". Possible wrong data.`
        };
    }

    return { valid: true, matchedCount };
}

// Actor IDs for each platform
const ACTOR_IDS: Record<string, string> = {
  linkedin: 'apimaestro/linkedin-profile-posts',
  youtube: 'apidojo/youtube-scraper',
  x: 'gentle_cloud/twitter-tweets-scraper',
  instagram: 'apify/instagram-scraper',
};

// Build actor-specific input
function buildActorInput(platform: string, url: string, postCount: number): Record<string, unknown> {
  switch (platform) {
    case 'linkedin': return { username: url, limit: postCount };
    case 'youtube': return { startUrls: [{ url }], maxItems: postCount };
    case 'x': return { result_count: String(postCount), start_urls: [{ url }] };
    // Instagram: directUrls mode - NO searchType needed (that's only for search queries)
    case 'instagram': return { 
        directUrls: [url], 
        resultsLimit: postCount,
        resultsType: 'posts'
    };
    default: throw new Error(`Unknown platform: ${platform}`);
  }
}

interface ScrapeRequest {
    platform: string;
    url: string;
    postCount: number;
    userId?: string;
    requestId?: string;
    emailNotificationsEnabled?: boolean;
    scheduleId?: string; // For scheduled scrapes - tracks which schedule triggered this
}

export interface AsyncScrapeResult {
    success: boolean;
    apifyRunId?: string;
    error?: string;
    insufficientFunds?: boolean;
    requiredFunds?: number;
}


export interface ScrapeResult {
    success: boolean;
    cost: number;
    itemsProcessed: number;
    logStatus: string;
    emailSent: boolean;
    error?: string;
    excelBuffer?: Buffer;
    fileName?: string;
    deductionStatus: 'ok' | 'skipped' | 'failed' | 'insufficient_funds';
}

// Keep synchronous execution for backward compatibility if needed, using the inner logic
// But simplified to reuse components if we were to refactor strictly.
// For now, let's keep executeScrape as is (Synchronous) but add startAsyncScrape below it.

export async function executeScrape(params: ScrapeRequest): Promise<ScrapeResult> {
    // ... [existing sync implementation code] ...
    // Since I messed up the write_to_file content in the previous turn by omitting the existing sync function 
    // I need to provide the FULL file content.
    // However, I want to move towards async.
    // Let's implement startAsyncScrape and processScrapeResult
    // And I will keep executeScrape for the Cron job (Cron can be sync or async, actually async is better there too for timeouts).
    // Let's replace executeScrape entirely with the sync version logic I wrote before, OR
    // better yet, let the Cron job use the async startAsyncScrape too?
    // If Cron uses async, it won't timeout. But Cron expects to finish in 1 function? 
    // No, Cron trigger just starts it.
    
    // For now, I will RE-INCLUDE `executeScrape` fully to avoid breaking existing imports in `api/scrape`.
    // Then I will ADD the new async functions.
    
    const { platform, url, postCount, userId, requestId } = params;

    const actorId = ACTOR_IDS[platform];
    if (!actorId) {
       throw new Error(`Invalid platform: ${platform}`);
    }

    // Rate calculation
    const rate = PLATFORM_RATES[platform] || 1.5;
    const estimatedCost = Number((rate * postCount).toFixed(2));

    // --- IDEMPOTENCY CHECK ---
    if (requestId && userId) {
      const { data: existingScrape } = await getSupabase(true)
        .from('scrapes')
        .select('id, status')
        .eq('request_id', requestId)
        .single();

      if (existingScrape) {
        throw new Error('This request has already been processed.');
      }
    }

    // --- ABUSE DETECTION: Block if too many recent failures ---
    if (userId) {
        const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000).toISOString();
        const { data: recentFailures, error: failureError } = await getSupabase(true)
            .from('scrapes')
            .select('id')
            .eq('user_id', userId)
            .in('status', ['failed', 'validation_failed'])
            .gte('created_at', oneHourAgo);
        
        if (!failureError && recentFailures && recentFailures.length >= 3) {
            return {
                success: false,
                cost: 0,
                itemsProcessed: 0,
                logStatus: 'rate_limited',
                emailSent: false,
                error: `Too many failed attempts recently. Please wait before trying again, or contact support if this persists.`,
                deductionStatus: 'skipped'
            };
        }
    }

    // --- CREDIT CHECK ---
    let userBalance: number | null = null;
    let userEmail: string | null = null;
    let emailNotificationsEnabled = false;

    if (userId) {
      const { data: user, error: userError } = await getSupabase(true)
        .from('users')
        .select('balance, email, email_notifications')
        .eq('id', userId)
        .single();
      
      if (!userError && user) {
        userBalance = user.balance as number;
        userEmail = user.email as string;
        if (params.emailNotificationsEnabled !== undefined) {
             emailNotificationsEnabled = params.emailNotificationsEnabled;
        } else {
             emailNotificationsEnabled = user.email_notifications ?? true;
        }

        if (userBalance < estimatedCost) {
             return {
                success: false,
                cost: 0,
                itemsProcessed: 0,
                logStatus: 'skipped',
                emailSent: false,
                error: `Insufficient balance. Required: ₹${estimatedCost}, Available: ₹${userBalance}`,
                deductionStatus: 'insufficient_funds'
             };
        }
      }
    }

    // --- EXECUTION ---
    const apifyToken = process.env.APIFY_TOKEN;
    if (!apifyToken) throw new Error('APIFY_TOKEN not set');
    
    const client = new ApifyClient({ token: apifyToken });
    let items: Record<string, unknown>[] = [];
    
    try {
        const run = await client.actor(actorId).call(buildActorInput(platform, url, postCount));
        const dataset = await client.dataset(run.defaultDatasetId).listItems();
        items = dataset.items;
        
        if (!items || items.length === 0) {
            throw new Error('No data was returned from the scraper');
        }
    } catch (scrapeError: any) {
        return {
            success: false,
            cost: 0,
            itemsProcessed: 0,
            logStatus: 'failed_exec',
            emailSent: false,
            error: scrapeError.message || 'Scrape execution failed',
            deductionStatus: 'skipped'
        };
    }

    // --- VALIDATION: Verify scraped data matches requested target ---
    const validation = validateScrapedData(platform, url, items);
    if (!validation.valid) {
        console.warn(`[VALIDATION FAILED] ${validation.error}`);
        return {
            success: false,
            cost: 0,
            itemsProcessed: 0,
            logStatus: 'validation_failed',
            emailSent: false,
            error: validation.error || 'Data validation failed. No credits charged.',
            deductionStatus: 'skipped'
        };
    }

    // --- FINAL COST (only charged if validation passed) ---
    const actualCount = items.length;
    const cost = Number((rate * actualCount).toFixed(2));
    let deductionStatus: ScrapeResult['deductionStatus'] = 'skipped';

    // --- DEDUCTION ---
    if (userId && userBalance !== null) {
        const { error: deductError } = await getSupabase(true)
          .from('users')
          .update({ balance: userBalance - cost })
          .eq('id', userId);

        if (deductError) {
          console.error('Failed to deduct credits:', deductError);
          deductionStatus = 'failed';
        } else {
            deductionStatus = 'ok';
        }
    }

    // --- LOGGING ---
    let logStatus = 'skipped';
    if (userId) {
      const { error: insertError } = await getSupabase(true).from('scrapes').insert({
        user_id: userId,
        platform,
        target_url: url,
        post_count: actualCount,
        cost,
        request_id: requestId, 
        status: 'success',
      });

      if (insertError) {
          console.error('History insert failed:', insertError);
          logStatus = `failed: ${insertError.message}`;
      } else {
          logStatus = 'success';
          
          // Update stats
          await getSupabase(true).rpc('increment_user_stats', {
            user_id_param: userId,
            runs_increment: 1,
            data_points_increment: actualCount,
          });
      }
    }

    // --- RAW DATA PROCESS ---
    const excelData = mapDataToExcel(platform, items);
    const worksheet = XLSX.utils.json_to_sheet(excelData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Results');
    const maxWidths: number[] = [];
    excelData.forEach((row) => {
      Object.values(row).forEach((val, idx) => {
        const len = String(val).length;
        maxWidths[idx] = Math.max(maxWidths[idx] || 10, Math.min(len, 50));
      });
    });
    worksheet['!cols'] = maxWidths.map((w) => ({ wch: w }));
    const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
    const fileName = `${platform}_export_${Date.now()}.xlsx`;

    // --- EMAIL ---
    let emailSent = false;
    if (userId && userEmail && emailNotificationsEnabled) {
      try {
        emailSent = await sendScrapeResultEmail({
          to: userEmail,
          platform,
          target: url,
          count: actualCount,
          cost,
          fileName,
          fileBuffer: buffer as Buffer,
        });
      } catch (emailError) {
        console.error('Email failed:', emailError);
      }
    }

    return {
        success: true,
        cost,
        itemsProcessed: actualCount,
        logStatus,
        emailSent,
        excelBuffer: buffer as Buffer,
        fileName,
        deductionStatus
    };
}

// ------------------------------------------------------------------
// 1. ASYNC STARTER (Fire & Forget)
// ------------------------------------------------------------------
export async function startAsyncScrape(params: ScrapeRequest): Promise<AsyncScrapeResult> {
    const { platform, url, postCount, userId, requestId, scheduleId } = params;

    // A. Validate Platform
    const actorId = ACTOR_IDS[platform];
    if (!actorId) throw new Error(`Invalid platform: ${platform}`);

    // B. Calculate Estimated Cost
    const rate = PLATFORM_RATES[platform] || 1.5;
    const estimatedCost = Number((rate * postCount).toFixed(2));

    let userBalance: number | null = null;
    let userEmail: string | null = null; 

    // C. Credit Check & Idempotency
    if (userId) {
        if (requestId) {
            const { data: existing } = await getSupabase(true)
                .from('scrapes')
                .select('id')
                .eq('request_id', requestId)
                .single();
            if (existing) throw new Error('Duplicate request ID');
        }

        const { data: user } = await getSupabase(true)
            .from('users')
            .select('balance, email')
            .eq('id', userId)
            .single();
        
        if (user) {
            userBalance = user.balance as number;
            userEmail = user.email;
            if (userBalance < estimatedCost) {
                return { success: false, insufficientFunds: true, requiredFunds: estimatedCost };
            }
        }
    }

    // D. Trigger Apify (with Webhook)
    const apifyToken = process.env.APIFY_TOKEN;
    if (!apifyToken) throw new Error('APIFY_TOKEN missing');
    
    const client = new ApifyClient({ token: apifyToken });
    const webhookUrl = `${process.env.NEXT_PUBLIC_APP_URL || 'https://feed-me-delta.vercel.app'}/api/webhooks/apify?secret=${process.env.CRON_SECRET || 'default-secret'}`;

    try {
        const actorInput = buildActorInput(platform, url, postCount);
        
        // Start run with webhook
        const run = await client.actor(actorId).start(actorInput, {
            webhooks: [{
                eventTypes: ['ACTOR.RUN.SUCCEEDED', 'ACTOR.RUN.FAILED'],
                requestUrl: webhookUrl,
            }]
        });

        // E. Log "Processing" State to DB
        if (userId) {
            await getSupabase(true).from('scrapes').insert({
                user_id: userId,
                platform,
                target_url: url,
                post_count: 0, // Placeholder
                cost: 0,       // Placeholder until done
                status: 'processing', // NEW STATUS
                apify_run_id: run.id,
                request_id: requestId,
                schedule_id: scheduleId || null, // Link to schedule if this is a scheduled run
                // created_at defaults to now
            });
        }

        return { success: true, apifyRunId: run.id };

    } catch (e: any) {
        console.error('Async trigger failed:', e);
        return { success: false, error: e.message };
    }
}

// ------------------------------------------------------------------
// 2. WEBHOOK HANDLER (Completion Logic)
// ------------------------------------------------------------------
export async function processScrapeResult(runId: string) {
    console.log(`[Processor] Handling completion for run: ${runId}`);
    
    // A. Fetch Context from DB
    const { data: scrapeRecord } = await getSupabase(true)
        .from('scrapes')
        .select('*')
        .eq('apify_run_id', runId)
        .single();
    
    if (!scrapeRecord) {
        console.error(`No local record found for run ${runId}`);
        return;
    }

    if (scrapeRecord.status === 'success' || scrapeRecord.status === 'failed') {
        console.log('Already processed.');
        return;
    }

    const { user_id, platform, target_url } = scrapeRecord;
    const apifyToken = process.env.APIFY_TOKEN;
    const client = new ApifyClient({ token: apifyToken });

    try {
        // B. Fetch Items
        const run = await client.run(runId).get();
        if (!run) throw new Error('Run not found on Apify');

        if (run.status !== 'SUCCEEDED') {
            throw new Error(`Run failed with status: ${run.status}`);
        }

        const dataset = await client.dataset(run.defaultDatasetId).listItems();
        const items = dataset.items;
        const actualCount = items.length;

        // C. Calculate Final Cost
        const rate = PLATFORM_RATES[platform] || 1.5;
        const finalCost = Number((rate * actualCount).toFixed(2));

        // D. Deduct Credits
        const { data: user } = await getSupabase(true).from('users').select('balance, email, email_notifications').eq('id', user_id).single();
        if (user) {
             await getSupabase(true) 
                .from('users')
                .update({ balance: user.balance - finalCost })
                .eq('id', user_id);
        }

        // E. Generate Excel for email (we still need the buffer for email attachment)
        const excelData = mapDataToExcel(platform, items);
        const worksheet = XLSX.utils.json_to_sheet(excelData);
        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, worksheet, 'Results');
        
        // Auto-Size logic
        const maxWidths: number[] = [];
        excelData.forEach((row) => {
             Object.values(row).forEach((val, idx) => {
             const len = String(val).length;
             maxWidths[idx] = Math.max(maxWidths[idx] || 10, Math.min(len, 50));
          });
        });
        worksheet['!cols'] = maxWidths.map((w) => ({ wch: w }));
        
        const buffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });
        const fileName = `${platform}_${Date.now()}.xlsx`;

        // --- NO STORAGE UPLOAD ---
        // Instead, we use an on-demand download endpoint
        // The file_url points to our API which generates Excel on the fly
        // This means ZERO storage costs!
        
        // We need the scrape ID to construct the URL, so we update in two steps
        // First, update to success (the file_url will be set after we have the ID)
        
        console.log(`[Processor] Updating scrape ${scrapeRecord.id} with status=success`);
        
        // Construct on-demand download URL
        const appUrl = process.env.NEXT_PUBLIC_APP_URL || 'https://feed-me-delta.vercel.app';
        const fileUrl = `${appUrl}/api/download/${scrapeRecord.id}`;
        
        await getSupabase(true).from('scrapes').update({
            status: 'success',
            post_count: actualCount,
            cost: finalCost,
            file_url: fileUrl // Points to our on-demand download endpoint
        }).eq('id', scrapeRecord.id);

        console.log(`[Processor] Download URL set to: ${fileUrl}`);

        // Update Stats
        await getSupabase(true).rpc('increment_user_stats', {
            user_id_param: user_id,
            runs_increment: 1,
            data_points_increment: actualCount,
        });

        // Update scheduled_scrapes total_posts_caught if this was a scheduled run
        if (scrapeRecord.schedule_id) {
            const { data: schedule } = await getSupabase(true)
                .from('scheduled_scrapes')
                .select('total_posts_caught')
                .eq('id', scrapeRecord.schedule_id)
                .single();
            
            if (schedule) {
                await getSupabase(true)
                    .from('scheduled_scrapes')
                    .update({ 
                        total_posts_caught: (schedule.total_posts_caught || 0) + actualCount 
                    })
                    .eq('id', scrapeRecord.schedule_id);
            }
        }

        if (user && (user.email_notifications ?? true)) {
             await sendScrapeResultEmail({
                to: user.email,
                platform,
                target: target_url,
                count: actualCount,
                cost: finalCost,
                fileName,
                fileBuffer: buffer as Buffer
            });
        }

    } catch (error: any) {
        console.error(`Processing failed for ${runId}:`, error);
        // Mark as failed
        await getSupabase(true).from('scrapes').update({
            status: 'failed',
            cost: 0
        }).eq('id', scrapeRecord.id);
    }
}
