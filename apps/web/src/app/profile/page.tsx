'use client';

import { useState, useEffect } from 'react';
import { getSupabase, User } from '@/lib/supabase';
import {
  User as UserIcon,
  Calendar,
  Loader2,
  Check,
  AlertOctagon,
  HelpCircle,
  Zap,
  ShoppingBag,
  ArrowUpRight,
} from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn, formatShortIST } from '@/lib/utils';

type EngineJob = {
  id: number;
  handle: string;
  jobType: string;
  status: string;
  attempt: number;
  createdAt: string;
  updatedAt: string;
  lastError: string | null;
};

type EngineStats = {
  recentJobs: EngineJob[];
  totalFeeders: number;
  totalPosts: number;
  jobStats: { done: number; failed: number; pending: number; running: number };
};

export default function ProfilePage() {
  const [isEditing, setIsEditing] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [isDarkMode, setIsDarkMode] = useState(false);
  const [user, setUser] = useState<User | null>(null);
  const [formData, setFormData] = useState({ name: '', email: '' });
  const [emailNotifications, setEmailNotifications] = useState(true);
  const [engineStats, setEngineStats] = useState<EngineStats>({
    recentJobs: [],
    totalFeeders: 0,
    totalPosts: 0,
    jobStats: { done: 0, failed: 0, pending: 0, running: 0 },
  });

  useEffect(() => {
    async function fetchData() {
      setIsLoading(true);

      const { data: { user: authUser }, error: authError } = await getSupabase().auth.getUser();
      if (authError || !authUser) {
        window.location.href = '/login';
        return;
      }

      const userId = authUser.id;

      // Fetch user profile
      let { data: userData, error: userFetchError } = await getSupabase()
        .from('users')
        .select('*')
        .eq('id', userId)
        .single();

      if (!userData && userFetchError?.code === 'PGRST116') {
        const { data: newUser, error: createError } = await getSupabase()
          .from('users')
          .insert({
            id: userId,
            email: authUser.email || '',
            name: authUser.user_metadata?.full_name || authUser.email?.split('@')[0] || 'User',
            balance: 1000,
            total_runs: 0,
            data_points: 0,
            success_rate: 0,
            email_notifications: true,
          })
          .select()
          .single();

        if (createError) {
          alert(`Failed to initialize account: ${createError.message}`);
          setIsLoading(false);
          return;
        }
        userData = newUser;
      }

      if (userData) {
        setUser(userData as User);
        setEmailNotifications(userData.email_notifications ?? true);
        setFormData({
          name: userData.name || '',
          email: userData.email || '',
        });
      }

      // Fetch engine stats
      try {
        const res = await fetch('/api/profile/engine');
        if (res.ok) {
          const data = await res.json();
          setEngineStats(data);
        }
      } catch (e) {
        console.error('Failed to fetch engine stats', e);
      }

      setIsLoading(false);
    }

    fetchData();

    const savedTheme = localStorage.getItem('theme');
    const systemPrefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    const shouldBeDark = savedTheme === 'dark' || (!savedTheme && systemPrefersDark);
    setIsDarkMode(shouldBeDark);
    if (shouldBeDark) {
      document.documentElement.classList.add('dark');
      document.documentElement.classList.remove('light');
    } else {
      document.documentElement.classList.remove('dark');
      document.documentElement.classList.add('light');
    }
  }, []);

  const formatTimeAgo = (dateStr: string) => {
    const date = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1) return 'just now';
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    return `${Math.floor(diffHours / 24)}d ago`;
  };

  const statusBadge = (status: string) => {
    switch (status) {
      case 'done':
        return <span className="text-[10px] font-black bg-lime text-black px-2 py-0.5 uppercase border border-black">DONE</span>;
      case 'failed':
        return <span className="text-[10px] font-black bg-red-500 text-white px-2 py-0.5 uppercase">FAILED</span>;
      case 'running':
        return <span className="text-[10px] font-black bg-cyan-400 text-black px-2 py-0.5 uppercase border border-black animate-pulse">RUNNING</span>;
      default:
        return <span className="text-[10px] font-black bg-gray-200 dark:bg-gray-700 text-gray-600 dark:text-gray-300 px-2 py-0.5 uppercase">{status.toUpperCase()}</span>;
    }
  };

  const { jobStats } = engineStats;
  const totalJobs = jobStats.done + jobStats.failed + jobStats.pending + jobStats.running;
  const successRate = totalJobs > 0 ? Math.round((jobStats.done / totalJobs) * 100) : 0;

  return (
    <div className="h-[100dvh] w-screen overflow-y-auto pt-4 pb-24 md:pt-20 md:pb-8 scrollbar-hide">
      <div className="p-4 md:p-8 w-[94%] md:w-[88%] mx-auto space-y-6 md:space-y-8">

        {/* PAGE TITLE */}
        <div className="flex flex-col gap-1 mb-2">
          <h1 className="text-5xl md:text-7xl font-black italic uppercase tracking-tighter leading-none text-foreground">
            MY <span className="text-cyber-blue">PROFILE</span>
          </h1>
        </div>

        {/* ─── IDENTITY CARD ─── */}
        <div className="neo-card p-6 flex flex-col md:flex-row items-start md:items-center justify-between gap-6">
          <div className="flex items-center gap-6">
            <div className="w-20 h-20 border-3 border-black dark:border-white bg-lime flex items-center justify-center shrink-0 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] text-black overflow-hidden">
              {user?.avatar_url ? (
                <img src={user.avatar_url} alt={formData.name} className="w-full h-full object-cover" />
              ) : (
                <UserIcon size={36} strokeWidth={3} />
              )}
            </div>
            <div>
              {isEditing ? (
                <div className="space-y-3">
                  <input
                    type="text"
                    value={formData.name}
                    onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                    className="text-3xl font-black italic uppercase tracking-tighter leading-none text-foreground bg-transparent border-b-2 border-black dark:border-white outline-none w-full"
                    placeholder="YOUR NAME"
                  />
                  <input
                    type="text"
                    value={user?.avatar_url || ''}
                    onChange={(e) => setUser(prev => prev ? ({ ...prev, avatar_url: e.target.value }) : null)}
                    className="text-sm font-bold text-gray-500 bg-transparent border-b border-gray-300 outline-none w-full"
                    placeholder="Profile Image URL..."
                  />
                </div>
              ) : (
                <>
                  <h2 className="text-3xl md:text-4xl font-black italic uppercase tracking-tighter leading-none text-foreground">{formData.name}</h2>
                  <div className="flex items-center gap-3 mt-2">
                    <span className="px-2 py-1 text-[10px] font-black uppercase bg-black text-white dark:bg-white dark:text-black tracking-wider">
                      {formData.email}
                    </span>
                  </div>
                </>
              )}
            </div>
          </div>

          <button
            onClick={async () => {
              if (isEditing) {
                const { data: { user: authUser } } = await getSupabase().auth.getUser();
                if (!authUser) return;
                const { error } = await getSupabase()
                  .from('users')
                  .update({ name: formData.name, avatar_url: user?.avatar_url })
                  .eq('id', authUser.id);
                if (!error) setIsEditing(false);
                else alert(`Failed: ${error.message}`);
              } else {
                setIsEditing(true);
              }
            }}
            className="py-2 px-6 bg-white dark:bg-black text-foreground border-3 border-black dark:border-white font-black uppercase hover:bg-black hover:text-white dark:hover:bg-white dark:hover:text-black transition-colors shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] text-xs tracking-wider"
          >
            {isEditing ? 'Save' : 'Edit'}
          </button>
        </div>

        {/* ─── QUICK STATS BAR ─── */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {[
            { label: 'Handles', value: engineStats.totalFeeders },
            { label: 'Posts Tracked', value: engineStats.totalPosts },
            { label: 'Jobs Done', value: jobStats.done },
            { label: 'Success Rate', value: `${successRate}%` },
          ].map((stat) => (
            <div key={stat.label} className="border-3 border-black dark:border-white bg-white dark:bg-black p-4 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)]">
              <span className="block text-[9px] font-black uppercase tracking-widest opacity-50 mb-1">{stat.label}</span>
              <span className="block text-3xl md:text-4xl font-black tracking-tighter leading-none text-foreground">{stat.value}</span>
            </div>
          ))}
        </div>

        {/* ─── MAIN GRID ─── */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">

          {/* LEFT: RECENT ACTIVITY (8 cols) */}
          <div className="lg:col-span-8">
            <div className="border-3 border-black dark:border-white bg-white dark:bg-black shadow-hard">
              <div className="p-4 border-b-3 border-black dark:border-white">
                <h3 className="text-2xl font-black italic uppercase tracking-tighter leading-none text-foreground">
                  Recent Activity
                </h3>
                <span className="text-[10px] font-bold uppercase opacity-50 tracking-widest">Last 20 jobs</span>
              </div>

              <div className="overflow-x-auto overflow-y-auto max-h-[500px] scrollbar-thin">
                <table className="w-full text-left border-collapse">
                  <thead>
                    <tr className="border-b-3 border-black dark:border-white text-[10px] uppercase font-black bg-gray-50 dark:bg-black text-foreground sticky top-0 z-10">
                      <th className="p-3">Handle</th>
                      <th className="p-3 text-center">Status</th>
                      <th className="p-3 text-right">When</th>
                    </tr>
                  </thead>
                  <tbody className="text-sm font-bold text-foreground">
                    {isLoading ? (
                      <tr>
                        <td colSpan={3} className="p-8 text-center text-gray-400">
                          <Loader2 className="animate-spin mx-auto" />
                        </td>
                      </tr>
                    ) : engineStats.recentJobs.length === 0 ? (
                      <tr>
                        <td colSpan={3} className="p-8 text-center text-gray-400 uppercase text-sm">
                          No activity yet
                        </td>
                      </tr>
                    ) : engineStats.recentJobs.map((job) => (
                      <tr key={job.id} className="border-b-2 border-gray-100 dark:border-gray-800 hover:bg-yellow-50 dark:hover:bg-gray-900 transition-colors">
                        <td className="p-3">
                          <span className="font-black uppercase">@{job.handle}</span>
                        </td>
                        <td className="p-3 text-center">
                          {statusBadge(job.status)}
                        </td>
                        <td className="p-3 text-right text-gray-400 text-xs uppercase">
                          {formatTimeAgo(job.updatedAt)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* RIGHT: PAYMENTS + PREFS + HOTLINE (4 cols) */}
          <div className="lg:col-span-4 flex flex-col gap-6">

            {/* ─── PAYMENT OPTIONS ─── */}
            <div className="border-3 border-black dark:border-white bg-white dark:bg-black p-5 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)]">
              <h3 className="text-xl font-black italic uppercase tracking-tighter leading-none text-foreground mb-4">
                Add to your feed
              </h3>

              <div className="space-y-3">
                {/* Add Handle */}
                <a
                  href="#"
                  onClick={(e) => { e.preventDefault(); alert('Payment link coming soon'); }}
                  className="block p-4 border-3 border-black dark:border-white bg-lime text-black hover:-translate-y-1 hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] transition-all group"
                >
                  <div className="flex justify-between items-center">
                    <div>
                      <span className="block font-black text-2xl leading-none">₹499</span>
                      <span className="block text-[10px] font-bold uppercase tracking-widest mt-1 opacity-70">Add a new handle</span>
                    </div>
                    <ShoppingBag size={28} strokeWidth={3} className="group-hover:rotate-12 transition-transform" />
                  </div>
                </a>

                {/* Top-up Posts */}
                <a
                  href="#"
                  onClick={(e) => { e.preventDefault(); alert('Payment link coming soon'); }}
                  className="block p-4 border-3 border-black dark:border-white bg-white dark:bg-zinc-900 text-foreground hover:-translate-y-1 hover:shadow-[6px_6px_0px_0px_rgba(0,0,0,1)] dark:hover:shadow-[6px_6px_0px_0px_rgba(255,255,255,1)] transition-all group"
                >
                  <div className="flex justify-between items-center">
                    <div>
                      <span className="block font-black text-2xl leading-none">₹99</span>
                      <span className="block text-[10px] font-bold uppercase tracking-widest mt-1 opacity-70">Top up 15 posts</span>
                    </div>
                    <Zap size={28} strokeWidth={3} className="group-hover:scale-110 transition-transform fill-current" />
                  </div>
                </a>
              </div>
            </div>

            {/* ─── PREFERENCES ─── */}
            <div className="border-3 border-black dark:border-white bg-white dark:bg-black p-5 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)]">
              <h3 className="text-xl font-black italic uppercase tracking-tighter leading-none text-foreground mb-4">
                Preferences
              </h3>
              <div className="space-y-3">
                {[
                  {
                    label: 'Email Alerts',
                    value: emailNotifications,
                    action: async () => {
                      const newValue = !emailNotifications;
                      setEmailNotifications(newValue);
                      await getSupabase().from('users').update({ email_notifications: newValue }).eq('id', user?.id);
                    },
                    activeColor: 'bg-lime',
                  },
                  {
                    label: 'Dark Mode',
                    value: isDarkMode,
                    action: () => {
                      const newMode = !isDarkMode;
                      setIsDarkMode(newMode);
                      if (newMode) {
                        document.documentElement.classList.add('dark');
                        localStorage.setItem('theme', 'dark');
                      } else {
                        document.documentElement.classList.remove('dark');
                        document.documentElement.classList.add('light');
                        localStorage.setItem('theme', 'light');
                      }
                    },
                    activeColor: 'bg-pink',
                  },
                ].map((item, index) => (
                  <div
                    key={index}
                    onClick={item.action}
                    className="flex items-center justify-between p-3 border-3 border-black dark:border-white bg-white dark:bg-zinc-900 hover:translate-x-[2px] hover:translate-y-[2px] hover:shadow-none shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] transition-all cursor-pointer select-none"
                  >
                    <span className="font-black uppercase text-sm tracking-wider flex items-center gap-3 text-foreground">
                      <div className={cn(
                        'w-4 h-4 border-2 border-black dark:border-white transition-colors',
                        item.value ? item.activeColor : 'bg-transparent'
                      )} />
                      {item.label}
                    </span>
                    <div className={cn(
                      'w-7 h-7 border-3 border-black dark:border-white flex items-center justify-center transition-colors',
                      item.value ? 'bg-black dark:bg-white' : 'bg-white dark:bg-black'
                    )}>
                      {item.value && <Check size={16} className="text-white dark:text-black" strokeWidth={5} />}
                    </div>
                  </div>
                ))}
              </div>
            </div>

            {/* ─── HOTLINE ─── */}
            <div className="border-3 border-black dark:border-white bg-gray-100 dark:bg-zinc-900 p-4 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] flex flex-col gap-4 flex-grow">
              <div className="flex items-center gap-2 border-b-2 border-black dark:border-white pb-2">
                <div className="w-2.5 h-2.5 bg-red-500 rounded-full animate-ping" />
                <span className="font-black uppercase text-lg tracking-widest text-foreground">Help</span>
              </div>

              <div className="grid grid-cols-2 gap-3 flex-grow">
                <a
                  href="mailto:support@feedme.com?subject=Bug Report"
                  className="flex flex-col justify-center gap-2 bg-red-600 border-3 border-black dark:border-white p-4 hover:-translate-y-1 hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] transition-all group min-h-[120px]"
                >
                  <AlertOctagon className="text-white w-7 h-7 group-hover:rotate-12 transition-transform" strokeWidth={3} />
                  <div>
                    <span className="block font-black uppercase text-white text-lg leading-none">Report Bug</span>
                    <span className="block font-bold uppercase text-red-200 text-[9px] tracking-wide mt-1">Something broken?</span>
                  </div>
                </a>

                <a
                  href="mailto:billing@feedme.com?subject=Billing Help"
                  className="flex flex-col justify-center gap-2 bg-white dark:bg-black border-3 border-black dark:border-white p-4 hover:-translate-y-1 hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:hover:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] transition-all group min-h-[120px]"
                >
                  <HelpCircle className="text-foreground w-7 h-7 group-hover:scale-110 transition-transform" strokeWidth={3} />
                  <div>
                    <span className="block font-black uppercase text-foreground text-lg leading-none">Billing</span>
                    <span className="block font-bold uppercase text-gray-500 text-[9px] tracking-wide mt-1">Payment help</span>
                  </div>
                </a>
              </div>
            </div>

          </div>
        </div>

      </div>
    </div>
  );
}
