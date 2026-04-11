import { NextRequest, NextResponse } from 'next/server';
import { createClient as createSupabaseClient } from '@supabase/supabase-js';
import { createClient } from '@/lib/supabase/server';

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

export async function POST(request: NextRequest) {
  try {
    const auth = await requireUser();
    if (auth.error || !auth.user) return auth.error;

    const body = await request.json().catch(() => ({}));
    const endpoint = typeof body?.endpoint === 'string' ? body.endpoint.trim() : '';
    const p256dh = typeof body?.keys?.p256dh === 'string' ? body.keys.p256dh.trim() : '';
    const authKey = typeof body?.keys?.auth === 'string' ? body.keys.auth.trim() : '';

    if (!endpoint || !p256dh || !authKey) {
      return NextResponse.json({ error: 'Invalid push subscription payload' }, { status: 400 });
    }

    const sb = adminClient();
    const { error: profileError } = await sb
      .from('users')
      .upsert({
        id: auth.user.id,
        email: auth.user.email || '',
        name: auth.user.user_metadata?.full_name || auth.user.email?.split('@')[0] || 'User',
        balance: 1000,
        email_notifications: true,
        fire_alert_threshold: 25,
        pwa_push_enabled: true,
      }, { ignoreDuplicates: true, onConflict: 'id' });

    if (profileError) {
      throw profileError;
    }

    const { error } = await sb
      .from('web_push_subscriptions')
      .upsert({
        auth_key: authKey,
        enabled: true,
        endpoint,
        last_error: null,
        last_seen_at: new Date().toISOString(),
        p256dh_key: p256dh,
        updated_at: new Date().toISOString(),
        user_agent: request.headers.get('user-agent') || null,
        user_id: auth.user.id,
      }, { onConflict: 'endpoint' });

    if (error) {
      throw error;
    }

    await sb
      .from('users')
      .update({ pwa_push_enabled: true })
      .eq('id', auth.user.id);

    return NextResponse.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to save push subscription';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function DELETE(request: NextRequest) {
  try {
    const auth = await requireUser();
    if (auth.error || !auth.user) return auth.error;

    const body = await request.json().catch(() => ({}));
    const endpoint = typeof body?.endpoint === 'string' ? body.endpoint.trim() : '';

    if (!endpoint) {
      return NextResponse.json({ error: 'Missing subscription endpoint' }, { status: 400 });
    }

    const sb = adminClient();
    const { error } = await sb
      .from('web_push_subscriptions')
      .delete()
      .eq('user_id', auth.user.id)
      .eq('endpoint', endpoint);

    if (error) {
      throw error;
    }

    const { count, error: countError } = await sb
      .from('web_push_subscriptions')
      .select('id', { count: 'exact', head: true })
      .eq('user_id', auth.user.id)
      .eq('enabled', true);

    if (countError) {
      throw countError;
    }

    if (!count) {
      await sb
        .from('users')
        .update({ pwa_push_enabled: false })
        .eq('id', auth.user.id);
    }

    return NextResponse.json({ ok: true });
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Failed to remove push subscription';
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
