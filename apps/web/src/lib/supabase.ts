import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { createBrowserClient } from '@supabase/ssr';

let supabaseInstance: SupabaseClient | null = null;
let supabaseAdminInstance: SupabaseClient | null = null;

export function getSupabase(admin = false): SupabaseClient {
  const isServer = typeof window === 'undefined';
  
  if (admin && !isServer) {
    throw new Error('Admin access is only available on the server');
  }

  if (admin) {
    if (supabaseAdminInstance) return supabaseAdminInstance;
    
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
    const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

    if (!supabaseUrl || !serviceRoleKey) {
      throw new Error('Supabase admin environment variables are not set');
    }

    supabaseAdminInstance = createClient(supabaseUrl, serviceRoleKey, {
      auth: {
        autoRefreshToken: false,
        persistSession: false
      }
    });
    return supabaseAdminInstance;
  }

  // For browser-side, use createBrowserClient from @supabase/ssr for proper cookie handling
  if (!isServer) {
    if (supabaseInstance) return supabaseInstance;
    
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

    if (!supabaseUrl || !supabaseAnonKey) {
      throw new Error('Supabase environment variables are not set');
    }

    supabaseInstance = createBrowserClient(supabaseUrl, supabaseAnonKey);
    return supabaseInstance;
  }

  // Server-side non-admin (shouldn't happen often but fallback)
  if (supabaseInstance) return supabaseInstance;

  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

  if (!supabaseUrl || !supabaseAnonKey) {
    throw new Error('Supabase environment variables are not set');
  }

  supabaseInstance = createClient(supabaseUrl, supabaseAnonKey);
  return supabaseInstance;
}

// Types for our database tables
export interface User {
  id: string;
  email: string;
  name: string;
  balance: number;
  total_runs: number;
  data_points: number;
  success_rate: number;
  email_notifications: boolean;
  avatar_url?: string;
  twitter_posts_caught?: number;
  reddit_posts_caught?: number;
  created_at: string;
}

export interface Scrape {
  id: string;
  user_id: string;
  platform: 'linkedin' | 'youtube' | 'x' | 'instagram';
  target_url: string;
  post_count: number;
  cost: number;
  status: 'pending' | 'success' | 'failed';
  created_at: string;
}

// Platform rates (₹ per post)
export const PLATFORM_RATES: Record<string, number> = {
  linkedin: 2.25,
  youtube: 2,
  x: 1.5,
  instagram: 1.75,
};
