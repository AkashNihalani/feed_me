import { NextRequest, NextResponse } from 'next/server';
import crypto from 'crypto';
import { getSupabase } from '@/lib/supabase';

// Razorpay sends raw body, we need to handle it properly
export async function POST(request: NextRequest) {
  try {
    const body = await request.text();
    const signature = request.headers.get('x-razorpay-signature');

    if (!signature) {
      console.error('[WEBHOOK] Missing Razorpay signature');
      return NextResponse.json({ error: 'Missing signature' }, { status: 400 });
    }

    // Verify webhook signature
    const webhookSecret = process.env.RAZORPAY_WEBHOOK_SECRET;
    if (!webhookSecret) {
      console.error('[WEBHOOK] RAZORPAY_WEBHOOK_SECRET not configured');
      return NextResponse.json({ error: 'Server configuration error' }, { status: 500 });
    }

    const expectedSignature = crypto
      .createHmac('sha256', webhookSecret)
      .update(body)
      .digest('hex');

    if (expectedSignature !== signature) {
      console.error('[WEBHOOK] Invalid signature');
      return NextResponse.json({ error: 'Invalid signature' }, { status: 400 });
    }

    // Parse the verified payload
    const event = JSON.parse(body);
    console.log('[WEBHOOK] Received event:', event.event);

    // Handle payment_link.paid event
    if (event.event === 'payment_link.paid') {
      const paymentLink = event.payload.payment_link.entity;
      const payment = event.payload.payment.entity;

      const paymentLinkId = paymentLink.id;
      const userId = paymentLink.notes?.user_id;
      const amountPaise = payment.amount; // Already in paise
      const amountRupees = amountPaise / 100;
      const razorpayPaymentId = payment.id;

      console.log('[WEBHOOK] Payment received:', {
        paymentLinkId,
        userId,
        amountRupees,
        razorpayPaymentId,
      });

      if (!userId && !payment.email) {
        console.error('[WEBHOOK] Missing both user_id in notes and payment.email');
        return NextResponse.json({ error: 'Missing user identification' }, { status: 400 });
      }

      // Use admin client to bypass RLS
      const supabase = getSupabase(true);
      let targetUserId = userId;

      // If no userId from notes, finding user by email
      if (!targetUserId && payment.email) {
        console.log('[WEBHOOK] Attempting to find user by email:', payment.email);
        const { data: userByEmail } = await supabase
          .from('users')
          .select('id')
          .eq('email', payment.email)
          .single();
        
        if (userByEmail) {
          targetUserId = userByEmail.id;
          console.log('[WEBHOOK] Found user by email match:', targetUserId);
        } else {
          console.error('[WEBHOOK] No user found for email:', payment.email);
          return NextResponse.json({ error: 'User not found' }, { status: 404 });
        }
      }

      // Check idempotency - don't process same payment twice (by payment ID, not link ID)
      const { data: existingTx } = await supabase
        .from('transactions')
        .select('status')
        .eq('razorpay_payment_id', razorpayPaymentId)
        .single();

      if (existingTx?.status === 'paid') {
        console.log('[WEBHOOK] Payment already processed, skipping:', razorpayPaymentId);
        return NextResponse.json({ success: true, message: 'Already processed' });
      }

      // Insert or update transaction record
      // For API-created links, this updates the existing pending record
      // For Dashboard-created links, this creates a new record
      await supabase
        .from('transactions')
        .upsert({
          id: paymentLinkId,
          user_id: targetUserId,
          amount: amountPaise,
          status: 'paid',
          razorpay_payment_id: razorpayPaymentId,
          updated_at: new Date().toISOString(),
        }, { onConflict: 'id' });

      // Increment user balance
      const { data: currentUser } = await supabase
        .from('users')
        .select('balance')
        .eq('id', targetUserId)
        .single();

      const newBalance = (currentUser?.balance || 0) + amountRupees;

      await supabase
        .from('users')
        .update({ balance: newBalance })
        .eq('id', targetUserId);

      console.log('[WEBHOOK] Balance updated for user:', targetUserId, 'Amount:', amountRupees, 'New balance:', newBalance);

      return NextResponse.json({ success: true });
    }

    // Handle payment.failed event
    if (event.event === 'payment.failed') {
      const payment = event.payload.payment.entity;
      console.log('[WEBHOOK] Payment failed:', payment.id);

      // We could update transaction status here if needed
      return NextResponse.json({ success: true, message: 'Payment failure noted' });
    }

    // Acknowledge other events
    return NextResponse.json({ success: true, message: 'Event received' });

  } catch (error: any) {
    console.error('[WEBHOOK] Error processing webhook:', error);
    return NextResponse.json(
      { error: error.message || 'Webhook processing failed' },
      { status: 500 }
    );
  }
}

// Razorpay may send GET requests for verification
export async function GET() {
  return NextResponse.json({ status: 'Razorpay webhook endpoint active' });
}
