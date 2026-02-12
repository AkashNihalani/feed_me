import { NextRequest, NextResponse } from 'next/server';
import { startAsyncScrape } from '@/lib/scraping';

export const maxDuration = 60; // Standard duration is fine for fire-and-forget

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { platform, url, postCount, userId, requestId } = body;

    // Validate inputs
    if (!platform || !url || !postCount) {
      return NextResponse.json(
        { error: 'Missing required fields: platform, url, postCount' },
        { status: 400 }
      );
    }

    // --- ASYNC EXECUTION ---
    const result = await startAsyncScrape({
        platform,
        url,
        postCount: Number(postCount),
        userId,
        requestId,
        emailNotificationsEnabled: true // Default to true for manual
    });

    if (!result.success) {
        // Map service errors
        const status = result.insufficientFunds ? 402 : 500;
        return NextResponse.json(
            { error: result.error || 'Failed to start scrape' },
            { status }
        );
    }

    // Return immediate success
    return NextResponse.json({ 
        success: true, 
        message: 'Scrape started successfully', 
        runId: result.apifyRunId 
    });

  } catch (error) {
    console.error('Scrape API error:', error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : 'An unexpected error occurred' },
      { status: 500 }
    );
  }
}
