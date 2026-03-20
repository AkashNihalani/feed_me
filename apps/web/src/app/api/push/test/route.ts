import { NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';
import { clampAlertThreshold } from '@/lib/fireAlertSettings';

export const dynamic = 'force-dynamic';

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

export async function POST() {
  try {
    const supabase = await createClient();
    const {
      data: { user },
      error: authError,
    } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json({ error: 'Unauthorized' }, { status: 401 });
    }

    const sb = adminClient();
    const { data: profile, error: profileError } = await sb
      .from('users')
      .select('fire_alert_threshold, pwa_push_enabled')
      .eq('id', user.id)
      .maybeSingle();

    if (profileError) {
      throw profileError;
    }

    const threshold = clampAlertThreshold(profile?.fire_alert_threshold ?? 25);
    const enabled = Boolean(profile?.pwa_push_enabled);

    if (!enabled) {
      return NextResponse.json({ error: 'Enable Fire Alerts first.' }, { status: 400 });
    }

    const { count, error: subscriptionError } = await sb
      .from('web_push_subscriptions')
      .select('id', { count: 'exact', head: true })
      .eq('user_id', user.id)
      .eq('enabled', true);

    if (subscriptionError) {
      throw subscriptionError;
    }

    if (!count) {
      return NextResponse.json({ error: 'No active push subscription found.' }, { status: 400 });
    }

    const timestamp = Date.now();
    const { error } = await sb
      .from('web_push_jobs')
      .insert({
        dedupe_key: `test:${user.id}:${timestamp}`,
        kind: 'test',
        payload: {
          body: `Fire alerts below ${threshold}% will arrive here.`,
          tag: 'feedme-test-ping',
          title: 'FeedMe test ping',
          url: `/fire?threshold=${encodeURIComponent(String(threshold))}`,
        },
        user_id: user.id,
      });

    if (error) {
      throw error;
    }

    return NextResponse.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to queue test push';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
