import { NextRequest, NextResponse } from 'next/server';
import Razorpay from 'razorpay';
import { getSupabase } from '@/lib/supabase';

export async function POST(request: NextRequest) {
  try {
    // Initialize Razorpay inside handler to avoid build-time env errors
    const razorpay = new Razorpay({
      key_id: process.env.RAZORPAY_KEY_ID!,
      key_secret: process.env.RAZORPAY_KEY_SECRET!,
    });

    const { amount } = await request.json();

    // Validate amount (minimum ₹100)
    if (!amount || amount < 100) {
      return NextResponse.json(
        { error: 'Minimum top-up amount is ₹100' },
        { status: 400 }
      );
    }

    // Get authenticated user
    const supabase = getSupabase();
    const { data: { user }, error: authError } = await supabase.auth.getUser();

    if (authError || !user) {
      return NextResponse.json(
        { error: 'Authentication required' },
        { status: 401 }
      );
    }

    // Fetch user email for prefill
    const { data: userData } = await supabase
      .from('users')
      .select('email, name')
      .eq('id', user.id)
      .single();

    // Create Razorpay Payment Link
    const paymentLink = await razorpay.paymentLink.create({
      amount: amount * 100, // Convert to paise
      currency: 'INR',
      accept_partial: false,
      description: `Feed Me Wallet Top-up: ₹${amount}`,
      customer: {
        name: userData?.name || 'User',
        email: userData?.email || user.email,
      },
      notify: {
        sms: false,
        email: true,
      },
      callback_url: `${process.env.NEXT_PUBLIC_BASE_URL || 'https://feed-me-delta.vercel.app'}/profile?payment=success`,
      callback_method: 'get',
      notes: {
        user_id: user.id,
        amount_rupees: amount.toString(),
      },
    });

    // Store transaction in database with 'pending' status
    const supabaseAdmin = getSupabase(true);
    await supabaseAdmin.from('transactions').insert({
      id: paymentLink.id,
      user_id: user.id,
      amount: amount * 100, // Store in paise
      status: 'pending',
    });

    return NextResponse.json({
      success: true,
      paymentLinkId: paymentLink.id,
      paymentLinkUrl: paymentLink.short_url,
    });

  } catch (error: any) {
    console.error('[PAYMENT] Error creating payment link:', error);
    return NextResponse.json(
      { error: error.message || 'Failed to create payment link' },
      { status: 500 }
    );
  }
}
