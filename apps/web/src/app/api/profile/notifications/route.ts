import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient, type User as AuthUser } from '@supabase/supabase-js';
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

async function requireUser() {
  const supabase = await createClient();
  const {
    data: { user },
    error,
  } = await supabase.auth.getUser();

  if (error || !user) {
    return { error: NextResponse.json({ error: 'Unauthorized' }, { status: 401 }), user: null };
  }

  return { error: null, user };
}

async function ensureUserProfile(sb: ReturnType<typeof adminClient>, user: AuthUser) {
  const { data, error } = await sb
    .from('users')
    .select('id, fire_alert_threshold, pwa_push_enabled')
    .eq('id', user.id)
    .maybeSingle();

  if (error) {
    throw error;
  }

  if (data) {
    return data;
  }

  const { data: created, error: createError } = await sb
    .from('users')
    .insert({
      id: user.id,
      email: user.email || '',
      name: user.user_metadata?.full_name || user.email?.split('@')[0] || 'User',
      balance: 1000,
      email_notifications: true,
      fire_alert_threshold: 25,
      pwa_push_enabled: false,
    })
    .select('id, fire_alert_threshold, pwa_push_enabled')
    .single();

  if (createError) {
    throw createError;
  }

  return created;
}

export async function GET() {
  try {
    const auth = await requireUser();
    if (auth.error || !auth.user) return auth.error;

    const sb = adminClient();
    const profile = await ensureUserProfile(sb, auth.user);
    const { count, error: subscriptionError } = await sb
      .from('web_push_subscriptions')
      .select('id', { count: 'exact', head: true })
      .eq('user_id', auth.user.id)
      .eq('enabled', true);

    if (subscriptionError) {
      throw subscriptionError;
    }

    return NextResponse.json({
      fireAlertThreshold: clampAlertThreshold(profile.fire_alert_threshold ?? 25),
      hasActiveSubscription: (count || 0) > 0,
      pwaPushEnabled: Boolean(profile.pwa_push_enabled),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to load notification settings';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function PUT(request: NextRequest) {
  try {
    const auth = await requireUser();
    if (auth.error || !auth.user) return auth.error;

    const body = await request.json().catch(() => ({}));
    const nextThreshold = body.fireAlertThreshold == null
      ? undefined
      : clampAlertThreshold(Number(body.fireAlertThreshold));
    const nextPushEnabled = body.pwaPushEnabled == null
      ? undefined
      : Boolean(body.pwaPushEnabled);

    if (nextThreshold == null && nextPushEnabled == null) {
      return NextResponse.json({ error: 'Nothing to update' }, { status: 400 });
    }

    const sb = adminClient();
    await ensureUserProfile(sb, auth.user);

    const updates: Record<string, boolean | number> = {};
    if (nextThreshold != null) updates.fire_alert_threshold = nextThreshold;
    if (nextPushEnabled != null) updates.pwa_push_enabled = nextPushEnabled;

    const { data, error } = await sb
      .from('users')
      .update(updates)
      .eq('id', auth.user.id)
      .select('fire_alert_threshold, pwa_push_enabled')
      .single();

    if (error) {
      throw error;
    }

    if (nextPushEnabled === false) {
      const now = new Date().toISOString();
      await sb
        .from('web_push_jobs')
        .update({
          last_error: 'PWA push disabled',
          status: 'skipped',
          updated_at: now,
        })
        .eq('user_id', auth.user.id)
        .in('status', ['pending', 'retry', 'running']);
    }

    return NextResponse.json({
      fireAlertThreshold: clampAlertThreshold(data.fire_alert_threshold ?? 25),
      pwaPushEnabled: Boolean(data.pwa_push_enabled),
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to update notification settings';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
