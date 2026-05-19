import { NextRequest, NextResponse } from 'next/server';
import feederPoolSample from '@/data/feeder_pool_metric_attached_sample.json';
import { createClient } from '@/lib/supabase/server';

export const dynamic = 'force-dynamic';

function normalizeHandle(value: string | null): string {
  return (value || '').trim().replace(/^@+/, '').toLowerCase();
}

export async function GET(request: NextRequest) {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const handle = normalizeHandle(request.nextUrl.searchParams.get('handle'));
    const accounts = Array.isArray(feederPoolSample.accounts) ? feederPoolSample.accounts : [];
    const filtered = handle && handle !== 'all'
      ? accounts.filter((account) => normalizeHandle(account.feeder?.handle || null) === handle)
      : accounts;

    return NextResponse.json({
      accounts: filtered,
      source: {
        kind: 'first_compile_local_artifact',
        path: 'apps/web/src/data/feeder_pool_metric_attached_sample.json',
      },
    });
  } catch (error) {
    return NextResponse.json(
      {
        accounts: [],
        error: error instanceof Error ? error.message : 'Unable to load feeder pool sample payload',
      },
      { status: 500 },
    );
  }
}
