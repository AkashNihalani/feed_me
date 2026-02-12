'use client';

import { useState, useEffect } from 'react';
import { Card } from '@/components/ui/Card';
import { getSupabase, Scrape, User } from '@/lib/supabase';
import { 
  User as UserIcon, 
  Calendar,
  Zap, 
  Download,
  IndianRupee,
  ArrowUpRight,
  Activity,
  Loader2,
  Trash2,
  Clock,
  Repeat,
  Check,
  ChevronDown,
  Linkedin,
  Youtube,
  Hash,
  Instagram,
  AlertOctagon,
  HelpCircle
} from 'lucide-react';
import { motion, AnimatePresence, useSpring, useTransform } from 'framer-motion';
import { cn, extractHandle, formatShortIST } from '@/lib/utils';

const PLATFORMS = [
  { id: 'linkedin', name: 'LinkedIn', icon: Linkedin, active: 'bg-blue-500' },
  { id: 'youtube', name: 'YouTube', icon: Youtube, active: 'bg-red-500' },
  { id: 'x', name: 'X', icon: Hash, active: 'bg-black dark:bg-white' },
  { id: 'instagram', name: 'Instagram', icon: Instagram, active: 'bg-pink' },
];

// Using real authentication

// Animated Counter Component
function AnimatedCounter({ value, className }: { value: number, className?: string }) {
    const spring = useSpring(value, { mass: 0.8, stiffness: 75, damping: 15 });
    const display = useTransform(spring, (current: number) => Math.round(current).toLocaleString());

    useEffect(() => {
        spring.set(value);
    }, [value, spring]);

    return <motion.span className={className}>{display}</motion.span>;
}

export default function ProfilePage() {
  const [isEditing, setIsEditing] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [isDarkMode, setIsDarkMode] = useState(false);
  const [activeTab, setActiveTab] = useState<'accounts' | 'posts'>('accounts');
  const [totalPosts, setTotalPosts] = useState(0);
  const [totalCredits, setTotalCredits] = useState(0);
  const [totalRuns, setTotalRuns] = useState(0);
  const [successRate, setSuccessRate] = useState(0);
  const [platformStats, setPlatformStats] = useState<Record<string, { accounts: Set<string>, posts: number }>>({});
  const [user, setUser] = useState<User | null>(null);
  const [scrapes, setScrapes] = useState<Scrape[]>([]);
  const [formData, setFormData] = useState({
    name: '',
    email: '',
  });

  const [topUpAmount, setTopUpAmount] = useState<number>(500);
  const [emailNotifications, setEmailNotifications] = useState(true);
  const [isPaymentLoading, setIsPaymentLoading] = useState(false);
  
  // Automated Runs State
  const [schedules, setSchedules] = useState<any[]>([]); // Using any for speed, ideally type it
  const [newSchedulePlatform, setNewSchedulePlatform] = useState('instagram');
  const [newScheduleUrl, setNewScheduleUrl] = useState('');
  const [newScheduleFrequency, setNewScheduleFrequency] = useState('weekly');
  const [isScheduleLoading, setIsScheduleLoading] = useState(false);
  const [activeDropdown, setActiveDropdown] = useState<'platform' | 'frequency' | null>(null);


  const [isPaymentModalOpen, setIsPaymentModalOpen] = useState(false);

  // Handle Razorpay Payment Link creation
  const handlePaymentLogic = async () => {
    if (topUpAmount < 100) {
      alert('Minimum top-up amount is ₹100');
      return;
    }

    setIsPaymentLoading(true);
    try {
      const response = await fetch('/api/payments/create-link', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ amount: topUpAmount }),
      });

      const data = await response.json();

      if (!response.ok) {
        // If API keys are missing, offer to open Razorpay Dashboard Payment Page
        if (data.error?.includes('key_id') || data.error?.includes('RAZORPAY_KEY')) {
          const staticUrl = process.env.NEXT_PUBLIC_RAZORPAY_PAYMENT_PAGE_URL;
          if (staticUrl) {
            // Construct URL with pre-filled values using Razorpay Payment Page format
            const paymentUrl = new URL(staticUrl);
            // Razorpay Payment Pages use 'prefill' format for email/contact
            // But 'amount' stays as-is (in paise)
            paymentUrl.searchParams.set('amount', (topUpAmount * 100).toString());
            paymentUrl.searchParams.set('prefill[email]', formData.email || user?.email || '');
            paymentUrl.searchParams.set('prefill[contact]', '9999999999');
            
            // Add custom notes if supported or just purely relies on email match
            // paymentUrl.searchParams.set('notes[user_id]', user?.id || ''); 

            // Direct redirect (not popup - avoids blockers)
            window.location.href = paymentUrl.toString();
            return;
          }
        }
        throw new Error(data.error || 'Failed to create payment link');
      }

      // Redirect to Razorpay payment page
      window.location.href = data.paymentLinkUrl;
    } catch (error: any) {
      console.error('[PAYMENT] Error:', error);
      alert(`Payment Error: ${error.message}`);
      setIsPaymentLoading(false);
    }
  };

  const handlePayment = () => {
      setIsPaymentModalOpen(true);
  };





  useEffect(() => {
    async function fetchData() {
      setIsLoading(true);

      // Get authenticated user
      const { data: { user: authUser }, error: authError } = await getSupabase().auth.getUser();

      if (authError || !authUser) {
        // Not authenticated, redirect to login
        alert('Please log in to view your profile.');
        window.location.href = '/login';
        return;
      }

      const userId = authUser.id;

      // Fetch user data
      let { data: userData, error: userFetchError } = await getSupabase()
        .from('users')
        .select('*')
        .eq('id', userId)
        .single();

      // If user doesn't exist in database, create them
      if (!userData && userFetchError?.code === 'PGRST116') {
        const newUserData = {
          id: userId,
          email: authUser.email || '',
          name: authUser.user_metadata?.full_name || authUser.email?.split('@')[0] || 'User',
          balance: 1000, // Give 1000 starting credits
          total_runs: 0,
          data_points: 0,
          success_rate: 0,
          email_notifications: true,
        };
        
        const { data: newUser, error: createError } = await getSupabase()
          .from('users')
          .insert(newUserData)
          .select()
          .single();

        if (createError) {
          alert(`Failed to initialize your account: ${createError.message}\n\nPlease check the browser console and contact support.`);
          setIsLoading(false);
          return;
        }
        
        userData = newUser;
      }

      if (userData) {
        setUser(userData as User);
        setEmailNotifications(userData.email_notifications ?? true);
        setFormData(prev => ({
          ...prev,
          name: userData.name || prev.name,
          email: userData.email || prev.email,
        }));
      }

      // Fetch recent scrapes for the table
      const { data: scrapesData } = await getSupabase()
        .from('scrapes')
        .select('*')
        .eq('user_id', userId)
        .order('created_at', { ascending: false })
        .limit(10);

      if (scrapesData) {
        setScrapes(scrapesData as Scrape[]);
      }

      // Calculate Stats (Sum of all successful runs + Success Rate)
      const { data: allHistory } = await getSupabase()
        .from('scrapes')
        .select('post_count, cost, status, platform, target_url')
        .eq('user_id', userId);

      if (allHistory && allHistory.length > 0) {
         const successfulRuns = allHistory.filter(s => s.status === 'success');
         const calculatedPosts = successfulRuns.reduce((acc, curr) => acc + (curr.post_count || 0), 0);
         const calculatedCredits = successfulRuns.reduce((acc, curr) => acc + (curr.cost || 0), 0);
         
         setTotalPosts(calculatedPosts);
         setTotalCredits(calculatedCredits);
         setTotalRuns(allHistory.length);
         setSuccessRate((successfulRuns.length / allHistory.length) * 100);

         // Calculate Platform Stats
         const stats = successfulRuns.reduce((acc, curr) => {
             const platform = curr.platform || 'unknown';
             if (!acc[platform]) {
                 acc[platform] = { accounts: new Set(), posts: 0 };
             }
             if (curr.target_url) acc[platform].accounts.add(curr.target_url);
             acc[platform].posts += (curr.post_count || 0);
             return acc;
         }, {} as Record<string, { accounts: Set<string>, posts: number }>);
         
         setPlatformStats(stats);
      }

      setIsLoading(false);
    }



    fetchData();

    // Check storage for dark mode preference
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

  // Fetch Schedules
  useEffect(() => {
    const fetchSchedules = async () => {
        const { data: { user } } = await getSupabase().auth.getUser();
        if (!user) return;
        
        try {
            const res = await fetch('/api/schedule');
            if (res.ok) {
                const data = await res.json();
                setSchedules(data.schedules || []);
            }
        } catch (e) {
            console.error('Failed to fetch schedules', e);
        }
    };
    fetchSchedules();
  }, []);

  const handleAddSchedule = async () => {
      if (!newScheduleUrl) return;
      setIsScheduleLoading(true);
      try {
          const res = await fetch('/api/schedule', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                  platform: newSchedulePlatform,
                  url: newScheduleUrl,
                  frequency: newScheduleFrequency
              })
          });
          const data = await res.json();
          if (res.ok) {
              setSchedules([data.schedule, ...schedules]);
              setNewScheduleUrl('');
          } else {
              alert(data.error);
          }
      } catch (e) {
          alert('Failed to schedule run');
      } finally {
          setIsScheduleLoading(false);
      }
  };

  const handleDeleteSchedule = async (id: string) => {
      const originalSchedules = [...schedules]; // Store for rollback
      try {
          // Optimistic update
          setSchedules(schedules.filter(s => s.id !== id));
          const res = await fetch(`/api/schedule?id=${id}`, { method: 'DELETE' });
          if (!res.ok) {
              const data = await res.json();
              throw new Error(data.error || 'Delete failed');
          }
      } catch (e) {
          console.error('Delete failed:', e);
          setSchedules(originalSchedules); // Rollback
          alert(`Failed to delete schedule: ${e instanceof Error ? e.message : 'Unknown error'}`);
      }
  };



  const formatStatsValue = (val: number | undefined) => {
     if (val === undefined) return '0';
     if (val < 1000) return val.toLocaleString();
     return `${(val / 1000).toFixed(1)}K`;
  };

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

  const stats = [
    { label: 'Total Runs', value: totalRuns.toLocaleString(), icon: Zap, color: 'bg-pink text-black' },
    { label: 'Posts Scraped', value: formatStatsValue(totalPosts), icon: Download, color: 'bg-cyan-400 text-black' },
    { label: 'Success Rate', value: `${successRate.toFixed(1)}%`, icon: Activity, color: 'bg-lime text-black' },
    { label: 'Credits Used', value: totalCredits.toLocaleString(), icon: IndianRupee, color: 'bg-yellow-400 text-black' }
  ];

  return (
    <div className="h-[100dvh] w-screen overflow-y-auto pt-4 pb-24 md:pt-20 md:pb-8 scrollbar-hide">
      <div className="p-4 md:p-8 w-[94%] md:w-[88%] mx-auto space-y-6 md:space-y-8">
       
       {/* Top Section: Identity & Quick Stats */}
       <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
       
        {/* HEADER for Fuel Page */}
        <div className="lg:col-span-4 flex flex-col gap-1 mb-2">
            <h1 className="text-5xl md:text-7xl font-black italic uppercase tracking-tighter leading-none text-foreground">
                FUEL <span className="text-cyber-blue">STATION</span>
            </h1>
            <p className="text-xs font-bold uppercase opacity-60 ml-1 text-foreground">
                Credits & Subscription • v1.0
            </p>
        </div>

        {/* Identity Card (Full Width) */}
        <div className="lg:col-span-4 neo-card p-6 flex items-center justify-between">
            <div className="flex items-center gap-6">
                <div className="w-24 h-24 border-3 border-black dark:border-white bg-lime flex items-center justify-center shrink-0 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] text-black overflow-hidden relative">
                    {user?.avatar_url ? (
                        <img src={user.avatar_url} alt={formData.name} className="w-full h-full object-cover" />
                    ) : (
                        <UserIcon size={40} strokeWidth={3} />
                    )}
                </div>
                <div>
                    {isEditing ? (
                        <div className="space-y-3">
                            <input 
                                type="text" 
                                value={formData.name}
                                onChange={(e) => setFormData(prev => ({ ...prev, name: e.target.value }))}
                                className="text-4xl font-black italic uppercase tracking-tighter leading-none text-foreground bg-transparent border-b-2 border-black dark:border-white outline-none w-full"
                                placeholder="YOUR NAME"
                            />
                            <input 
                                type="text" 
                                value={user?.avatar_url || ''}
                                onChange={(e) => setUser(prev => prev ? ({ ...prev, avatar_url: e.target.value }) : null)}
                                className="text-sm font-bold text-gray-500 bg-transparent border-b border-gray-300 outline-none w-full"
                                placeholder="Profile Image URL..."
                            />
                            <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wide">
                                Supports URLs from LinkedIn, Twitter/X, or Google Drive (Public). 
                                <span className="block opacity-50 mt-0.5">Note: Instagram links expire after a few days.</span>
                            </p>
                        </div>
                    ) : (
                        <>
                            <h1 className="text-4xl font-black italic uppercase tracking-tighter leading-none text-foreground">{formData.name}</h1>
                            <div className="flex items-center gap-4 mt-2">
                                <span className="px-2 py-1 text-[10px] font-black uppercase bg-black text-white dark:bg-white dark:text-black tracking-wider shadow-sm flex items-center gap-2">
                                    {formData.email}
                                </span>
                                <span className="text-sm font-bold text-gray-500 dark:text-gray-400 uppercase flex items-center gap-1">
                                    <Calendar size={14} /> Joined Jan 2026
                                </span>
                            </div>
                        </>
                    )}
                </div>
            </div>
            
            {/* Right Side: Balance & Actions (Stacked) */}
            <div className="flex flex-col items-end gap-3 min-w-[200px]">
                
                 {/* Massive Balance Stamp */}
                 <div className="bg-lime border-3 border-black dark:border-white shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] p-2 w-full text-center transform hover:-translate-y-1 hover:-translate-x-1 transition-transform cursor-default group">
                    <span className="block text-[9px] font-black uppercase tracking-widest opacity-60 mb-0.5 group-hover:opacity-100 transition-opacity text-black">
                        Current Funds
                    </span>
                    <div className="text-5xl font-black text-black leading-none tracking-tighter">
                        ₹{user?.balance?.toLocaleString() || 0}
                    </div>
                </div>

                <button 
                  onClick={async () => {
                      if (isEditing) {
                          const { data: { user: authUser } } = await getSupabase().auth.getUser();
                          if (!authUser) return;

                          const { error } = await getSupabase()
                              .from('users')
                              .update({ 
                                  name: formData.name,
                                  avatar_url: user?.avatar_url
                              })
                              .eq('id', authUser.id);
                          
                          if (!error) {
                              setIsEditing(false);
                          } else {
                              console.error(error);
                              alert(`Failed: ${error.message}`);
                          }
                      } else {
                          setIsEditing(true);
                      }
                  }}
                  className="w-full py-2 bg-white dark:bg-black text-foreground border-3 border-black dark:border-white font-black uppercase hover:bg-black hover:text-white dark:hover:bg-white dark:hover:text-black transition-colors shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] text-xs tracking-wider"
                >
                    {isEditing ? 'Save Changes' : 'Edit Profile'}
                </button>
            </div>
        </div>
      </div>

      {/* Main Dashboard Grid */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
        
        {/* Left Column: Activity & Stats (8 cols) */}
        <div className="lg:col-span-8 flex flex-col gap-6 h-full">

            {/* Live Activity Table */}
            <Card title="Live Activity Feed" className="flex-grow min-h-[500px] flex flex-col">
                <div className="overflow-x-auto overflow-y-auto flex-grow scrollbar-thin scrollbar-track-transparent scrollbar-thumb-gray-200 dark:scrollbar-thumb-gray-800">
                    <table className="w-full text-left border-collapse">
                        <thead>
                            <tr className="border-b-3 border-black dark:border-white text-xs uppercase font-black bg-gray-50 dark:bg-black text-black dark:text-white sticky top-0 z-10">
                                <th className="p-4">Platform</th>
                                <th className="p-4">Target</th>
                                <th className="p-4 text-center">Posts</th>
                                <th className="p-4">Status</th>
                                <th className="p-4 text-right">Cost</th>
                                <th className="p-4 text-right">Time</th>
                            </tr>
                        </thead>
                        <tbody className="text-sm font-bold text-foreground">
                            {isLoading ? (
                              <tr>
                                <td colSpan={6} className="p-8 text-center text-gray-400">
                                  <Loader2 className="animate-spin mx-auto" />
                                </td>
                              </tr>
                            ) : scrapes.length === 0 ? (
                              <tr>
                                <td colSpan={6} className="p-8 text-center text-gray-400 uppercase text-sm">
                                  No scrapes yet. Start feeding!
                                </td>
                              </tr>
                            ) : scrapes.map((scrape) => (
                                <tr key={scrape.id} className="border-b-2 border-gray-100 dark:border-gray-800 hover:bg-yellow-50 dark:hover:bg-gray-900 transition-colors group">
                                    <td className="p-4">
                                        <span className="flex items-center gap-2">
                                            <span className={cn(
                                              "w-2 h-2 rounded-full",
                                              scrape.platform === 'instagram' ? 'bg-pink' :
                                              scrape.platform === 'linkedin' ? 'bg-blue-500' :
                                              scrape.platform === 'youtube' ? 'bg-red-500' : 'bg-gray-500'
                                            )}/>
                                            {scrape.platform.charAt(0).toUpperCase() + scrape.platform.slice(1)}
                                        </span>
                                    </td>
                                    <td className="p-4 font-mono text-gray-600 dark:text-gray-400 group-hover:text-black dark:group-hover:text-white truncate max-w-[200px]">
                                      {extractHandle(scrape.target_url, scrape.platform)}
                                    </td>
                                    <td className="p-4 text-center">
                                        <span className="bg-black text-white dark:bg-white dark:text-black px-2 py-0.5 text-xs font-black">
                                          {scrape.post_count}
                                        </span>
                                    </td>
                                    <td className="p-4">
                                        <div className="flex items-center gap-2">
                                            {scrape.status === 'success' ? (
                                                <span className="text-xs font-black bg-lime text-black px-2 py-0.5 uppercase border border-black">SUCCESS</span>
                                            ) : (
                                                <span className="text-xs font-black bg-red-100 text-red-600 px-2 py-0.5 uppercase">
                                                    {scrape.status}
                                                </span>
                                            )}
                                        </div>
                                    </td>
                                    <td className="p-4 text-right font-mono font-bold">
                                        ₹{scrape.cost}
                                    </td>
                                    <td className="p-4 text-right text-gray-400 text-xs uppercase">
                                        {formatShortIST(scrape.created_at)}
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </Card>
            {/* FREQUENT FEEDERS (Bottom Left) */}
            <div className="border-3 border-black dark:border-white bg-white dark:bg-black p-6 shadow-hard relative group overflow-hidden">
                 <div className="flex justify-between items-start mb-6 relative z-10">
                    <h3 className="text-2xl font-black italic uppercase tracking-tighter leading-none flex items-center gap-2 text-foreground">
                        <Repeat className="stroke-current" />
                        FREQUENT FEEDERS
                    </h3>
                    <div className="px-2 py-1 bg-lime text-black text-[10px] font-black uppercase tracking-widest border border-black">
                        {schedules.length} Active
                    </div>
                </div>

                {/* CONTROL DECK (Interlocking Inputs) */}
                <div className="mb-6 relative z-30">
                     <div className="flex flex-col md:flex-row border-4 border-black dark:border-white shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] bg-white dark:bg-black">
                         
                         {/* 1. Platform Selector */}
                         <div className="relative w-full md:w-48 border-b-4 md:border-b-0 md:border-r-4 border-black dark:border-white">
                             <div 
                                onClick={() => setActiveDropdown(activeDropdown === 'platform' ? null : 'platform')}
                                className="h-14 flex justify-between items-center px-4 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-900 transition-colors"
                             >
                                 <div className="flex flex-col justify-center">
                                     <span className="text-[9px] font-black uppercase tracking-widest opacity-60 leading-none mb-1">PLATFORM</span>
                                     <span className="font-black uppercase text-lg truncate leading-none text-foreground">
                                        {newSchedulePlatform === 'x' ? 'X (Tw)' : newSchedulePlatform}
                                     </span>
                                 </div>
                                 <ChevronDown size={20} className={cn("transition-transform duration-300 stroke-[3px] text-foreground", activeDropdown === 'platform' ? "rotate-180" : "rotate-0")} />
                             </div>

                             <AnimatePresence>
                                 {activeDropdown === 'platform' && (
                                     <motion.div 
                                        initial={{ opacity: 0, y: 10 }}
                                        animate={{ opacity: 1, y: 0 }}
                                        exit={{ opacity: 0, y: 10 }}
                                        className="absolute top-[calc(100%+4px)] left-0 w-full bg-white dark:bg-black border-4 border-black dark:border-white shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] z-50 max-h-[200px] overflow-y-auto"
                                     >
                                        {['instagram', 'youtube', 'linkedin', 'x'].map((p) => (
                                            <div 
                                                key={p}
                                                onClick={() => {
                                                    setNewSchedulePlatform(p);
                                                    setActiveDropdown(null);
                                                }}
                                                className="p-3 font-bold uppercase hover:bg-lime hover:text-black cursor-pointer border-b-2 last:border-0 border-gray-100 dark:border-gray-800 transition-colors text-foreground"
                                            >
                                                {p === 'x' ? 'X (Twitter)' : p}
                                            </div>
                                        ))}
                                     </motion.div>
                                 )}
                             </AnimatePresence>
                         </div>

                         {/* 2. URL Input */}
                         <div className="flex-grow relative border-b-4 md:border-b-0 md:border-r-4 border-black dark:border-white">
                            <input 
                                type="text" 
                                placeholder="TARGET URL / HANDLE..."
                                value={newScheduleUrl}
                                onChange={(e) => setNewScheduleUrl(e.target.value)}
                                className="w-full h-14 px-4 bg-transparent font-black text-lg outline-none placeholder:text-gray-300 dark:placeholder:text-gray-700 text-foreground uppercase"
                             />
                         </div>

                         {/* 3. Frequency Selector */}
                         <div className="relative w-full md:w-32">
                             <div 
                                onClick={() => setActiveDropdown(activeDropdown === 'frequency' ? null : 'frequency')}
                                className="h-14 flex justify-between items-center px-4 cursor-pointer hover:bg-gray-50 dark:hover:bg-gray-900 transition-colors"
                             >
                                 <div className="flex flex-col justify-center">
                                     <span className="text-[9px] font-black uppercase tracking-widest opacity-60 leading-none mb-1">RATE</span>
                                     <span className="font-black uppercase text-lg truncate leading-none text-foreground">
                                        {newScheduleFrequency}
                                     </span>
                                 </div>
                                 <ChevronDown size={20} className={cn("transition-transform duration-300 stroke-[3px] text-foreground", activeDropdown === 'frequency' ? "rotate-180" : "rotate-0")} />
                             </div>

                             <AnimatePresence>
                                 {activeDropdown === 'frequency' && (
                                     <motion.div 
                                        initial={{ opacity: 0, y: 10 }}
                                        animate={{ opacity: 1, y: 0 }}
                                        exit={{ opacity: 0, y: 10 }}
                                        className="absolute top-[calc(100%+4px)] right-0 w-full min-w-[120px] bg-white dark:bg-black border-4 border-black dark:border-white shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] z-50 overflow-hidden"
                                     >
                                        {['daily', 'weekly'].map((f) => (
                                            <div 
                                                key={f}
                                                onClick={() => {
                                                    setNewScheduleFrequency(f);
                                                    setActiveDropdown(null);
                                                }}
                                                className="p-3 font-bold uppercase hover:bg-pink hover:text-black cursor-pointer border-b-2 last:border-0 border-gray-100 dark:border-gray-800 transition-colors text-foreground"
                                            >
                                                {f}
                                            </div>
                                        ))}
                                     </motion.div>
                                 )}
                             </AnimatePresence>
                         </div>
                     </div>

                     <button 
                       onClick={handleAddSchedule}
                       disabled={!newScheduleUrl || isScheduleLoading}
                       className="w-full mt-4 py-4 bg-black dark:bg-white text-white dark:text-black font-black uppercase text-lg border-2 border-transparent hover:border-lime shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] hover:translate-x-[-2px] hover:translate-y-[-2px] hover:shadow-[6px_6px_0px_0px_#bef264] transition-all flex items-center justify-center gap-3 disabled:opacity-50 disabled:grayscale"
                     >
                         {isScheduleLoading ? <Loader2 className="animate-spin" size={20}/> : <Zap size={20} className="fill-current"/>}
                         ADD FEEDER
                     </button>
                </div>

                {/* FEEDER LIST (High Density Strips) */}
                <div className="relative z-10 max-h-[500px] overflow-y-auto pr-2 scrollbar-thin scrollbar-thumb-black dark:scrollbar-thumb-white">
                    {schedules.map((schedule) => {
                      const platform = PLATFORMS.find(p => p.id === schedule.platform);
                      const handle = extractHandle(schedule.target_url, schedule.platform);
                      const nextRun = schedule.next_run_at ? new Date(schedule.next_run_at) : null;
                      
                      return (
                        <div 
                          key={schedule.id}
                          className="group relative border-b-4 last:border-b-0 border-black dark:border-white bg-transparent py-4 transition-all hover:bg-gray-50 dark:hover:bg-gray-900"
                        >
                            <div className="flex justify-between items-center">
                                {/* Left: Handle & Platform */}
                                <div className="flex flex-col">
                                    <div className="flex items-center gap-2 mb-1">
                                        <div className={cn("w-2 h-2 rounded-none", platform?.active || 'bg-gray-400')} />
                                        <span className="text-[10px] font-black uppercase tracking-widest opacity-60">
                                            {schedule.platform} • {schedule.frequency}
                                        </span>
                                    </div>
                                    <h4 className="font-black text-4xl uppercase tracking-tighter leading-none text-foreground truncate max-w-[250px] md:max-w-none" title={schedule.target_url}>
                                        {handle}
                                    </h4>
                                </div>

                                {/* Right: Metrics & Action */}
                                <div className="flex items-center gap-6">
                                    {/* Stats Block */}
                                    <div className="text-right hidden sm:block">
                                        <span className="block text-[9px] font-black uppercase tracking-widest opacity-40">TOTAL FED</span>
                                        <span className="block font-black text-2xl leading-none">
                                            {formatStatsValue(schedule.total_posts_caught)}
                                        </span>
                                    </div>

                                    <div className="text-right hidden sm:block">
                                        <span className="block text-[9px] font-black uppercase tracking-widest opacity-40">NEXT DROP</span>
                                        <div className="flex items-center justify-end gap-1">
                                            {nextRun && <div className="w-1.5 h-1.5 bg-green-500 animate-pulse" />}
                                            <span className="block font-bold text-xs leading-none">
                                                {nextRun ? formatShortIST(nextRun) : 'PENDING'}
                                            </span>
                                        </div>
                                    </div>

                                    {/* Delete Button */}
                                    <button
                                        onClick={() => handleDeleteSchedule(schedule.id)}
                                        className="w-10 h-10 flex items-center justify-center border-2 border-black dark:border-white bg-white dark:bg-black text-black dark:text-white hover:bg-red-600 hover:text-white transition-all opacity-0 group-hover:opacity-100"
                                        title="Sever Connection"
                                    >
                                        <Trash2 size={18} strokeWidth={3} />
                                    </button>
                                </div>
                            </div>
                        </div>
                      );
                  })}
                    {schedules.length === 0 && (
                        <div className="text-center py-12 border-4 border-dashed border-gray-200 dark:border-gray-800 text-gray-300 dark:text-gray-700">
                            <h4 className="font-black text-2xl uppercase italic opacity-50">NO ACTIVE FEEDERS</h4>
                            <p className="text-xs font-bold uppercase tracking-widest opacity-40 mt-2">Initialize above to begin</p>
                        </div>
                    )}
                </div>

                 {/* Decor */}
                 <div className="absolute -right-8 -bottom-8 opacity-[0.03] rotate-12 pointer-events-none">
                    <Repeat size={300} className="text-foreground"/>
                </div>
            </div>
        </div>

        {/* Right Column: Settings & Plan (4 cols) */}
        <div className="lg:col-span-4 flex flex-col gap-6 h-full">
            
            {/* THE STASH (Tabbed Activity Deck) */}
            <div className="bg-white dark:bg-zinc-900 text-black dark:text-white border-4 border-black dark:border-white p-3 shadow-[8px_8px_0px_0px_rgba(0,0,0,1)] dark:shadow-[8px_8px_0px_0px_rgba(255,255,255,1)]">
                <div className="flex justify-between items-start border-b-4 border-black dark:border-white pb-3 mb-3">
                     <div className="flex flex-col">
                        <h2 className="font-black text-2xl uppercase italic tracking-tighter leading-none mb-1">THE STASH</h2>
                        <span className="text-[10px] font-bold uppercase opacity-60 tracking-widest">Live Feed Stats</span>
                     </div>
                     
                     {/* MINI TABS */}
                     <div className="flex border-2 border-black dark:border-white bg-gray-100 dark:bg-black p-1 gap-1">
                        {['accounts', 'posts'].map((tab) => (
                            <button
                                key={tab}
                                onClick={() => setActiveTab(tab as 'accounts' | 'posts')}
                                className={cn(
                                    "px-3 py-1 text-[10px] font-black uppercase tracking-wider transition-all relative overflow-hidden",
                                    activeTab === tab ? "text-black dark:text-black" : "text-gray-400 hover:text-black dark:hover:text-white"
                                )}
                            >
                                {activeTab === tab && (
                                    <motion.div
                                        layoutId="activeTab"
                                        className="absolute inset-0 bg-lime mix-blend-multiply dark:mix-blend-normal"
                                        initial={false}
                                        transition={{ type: "spring", bounce: 0.2, duration: 0.6 }}
                                    />
                                )}
                                <span className={cn("relative z-10", activeTab === tab && "dark:text-black")}>{tab}</span>
                            </button>
                        ))}
                     </div>
                </div>

                <div className="space-y-4 min-h-[200px]">
                    <AnimatePresence mode="wait">
                        <motion.div
                            key={activeTab}
                            initial={{ opacity: 0, scale: 0.95 }}
                            animate={{ opacity: 1, scale: 1 }}
                            exit={{ opacity: 0, scale: 1.05 }}
                            transition={{ duration: 0.2 }}
                            className="grid grid-cols-2 gap-2"
                        >
                            {/* Render stats for each platform in a grid */}
                            {PLATFORMS.map((p) => {
                                const platform = p.id;
                                const stats = platformStats[platform] || { accounts: new Set(), posts: 0 };
                                const Icon = p.icon;
                                const value = activeTab === 'accounts' ? stats.accounts.size : stats.posts;
                                const label = activeTab === 'accounts' ? 'ACCOUNTS' : 'POSTS';

                                // Text color based on platform brand
                                const getBrandColor = (id: string) => {
                                    switch(id) {
                                        case 'instagram': return 'text-[#E1306C]'; // Official Insta Pink
                                        case 'linkedin': return 'text-[#0077B5]'; // Official LinkedIn Blue
                                        case 'youtube': return 'text-[#FF0000]'; // Official YouTube Red
                                        case 'x': return 'text-black dark:text-white';
                                        default: return 'text-black dark:text-white';
                                    }
                                };

                                return (
                                    <div key={platform} className="bg-white dark:bg-black border-2 border-black dark:border-white h-40 relative overflow-hidden">
                                        {/* Massive Icon - Watermark style */}
                                        <div className="absolute -right-4 -bottom-4 opacity-10 rotate-12">
                                            <Icon size={140} className="text-black dark:text-white" />
                                        </div>
                                        
                                        {/* Huge Icon - Foreground Accent */}
                                        <div className="absolute top-3 right-3">
                                            <Icon strokeWidth={2.5} size={32} className={getBrandColor(platform)} />
                                        </div>

                                        <div className="absolute bottom-2 left-3 flex flex-col z-10">
                                            <AnimatedCounter 
                                                value={value} 
                                                className={cn(
                                                    "block font-black text-7xl leading-[0.8] tracking-tighter",
                                                    getBrandColor(platform)
                                                )} 
                                            />
                                            <span className="block font-bold text-[10px] tracking-widest uppercase opacity-60 dark:text-white mt-1">
                                                {label}
                                            </span>
                                        </div>
                                    </div>
                                );
                            })}
                        </motion.div>
                    </AnimatePresence>
                </div>

                {/* TOTAL HAUL FOOTER */}
                <div className="mt-0 border-t-4 border-black dark:border-white bg-black dark:bg-white text-white dark:text-black p-3 flex justify-between items-center relative overflow-hidden group">
                     {/* Dynamic Total Calculation */}
                     {(() => {
                        const totalValue = activeTab === 'accounts' 
                            ? Object.values(platformStats).reduce((acc, curr) => acc + curr.accounts.size, 0)
                            : totalPosts;
                            
                        return (
                            <>
                                <div className="flex items-center gap-2 z-10">
                                    <div className="h-3 w-3 bg-lime animate-pulse rounded-full"></div>
                                    <span className="font-black text-xl uppercase tracking-widest italic">
                                        TOTAL {activeTab === 'accounts' ? 'ACCOUNTS' : 'HAUL'}
                                    </span>
                                </div>

                                <div className="flex items-baseline gap-1 z-10">
                                    <AnimatedCounter value={totalValue} className="font-black text-5xl leading-none tracking-tighter" />
                                    <span className="font-bold text-[10px] uppercase opacity-60">
                                        {activeTab === 'accounts' ? 'TARGETS' : 'UNITS'}
                                    </span>
                                </div>
                            </>
                        );
                     })()}
                </div>
            </div>

            {/* Deployment Funds Calculator (Yield Calculator) */}
            <div className="border-3 border-black dark:border-white bg-white dark:bg-black text-foreground p-6 shadow-hard flex flex-col relative overflow-hidden group">
                <div className="flex justify-between items-start mb-6">
                    <h3 className="text-2xl font-black italic uppercase tracking-tighter leading-none flex items-center gap-2">
                        <Zap className="fill-current" />
                        Feeding Budget
                    </h3>
                </div>

                {/* Input Section */}
                <div className="relative mb-8">
                    <label className="text-[10px] font-black uppercase tracking-widest opacity-60 mb-2 block">
                        Add Funds (INR)
                    </label>
                    <div className="flex items-stretch gap-4">
                        <div className={cn(
                            "flex-1 flex items-center border-b-4 transition-colors",
                             topUpAmount < 100 ? "border-red-500" : "border-black dark:border-white focus-within:border-lime"
                        )}>
                            <span className="text-4xl font-black text-gray-400 mr-2">₹</span>
                            <input 
                                type="number" 
                                min="100"
                                value={topUpAmount || ''}
                                onChange={(e) => setTopUpAmount(Number(e.target.value))}
                                placeholder="500" 
                                className="w-full bg-transparent text-6xl font-black outline-none placeholder:text-gray-200 dark:placeholder:text-gray-800 tabular-nums"
                            />
                        </div>

                        {/* Quick Toggles */}
                        <div className="flex flex-col gap-2 shrink-0">
                            {[100, 500].map((amount) => (
                                <button
                                    key={amount}
                                    onClick={() => setTopUpAmount(prev => (prev || 0) + amount)}
                                    className="px-3 py-1.5 bg-gray-100 dark:bg-gray-800 hover:bg-lime hover:text-black hover:border-black border-2 border-transparent transition-all font-black text-xs uppercase shadow-sm hover:translate-x-[-1px] hover:translate-y-[-1px] hover:shadow-[2px_2px_0px_0px_rgba(0,0,0,1)] opacity-60 hover:opacity-100"
                                >
                                    +{amount}
                                </button>
                            ))}
                        </div>
                    </div>

                    {topUpAmount < 100 && (
                        <p className="absolute -bottom-6 left-0 text-[10px] font-black uppercase text-red-500 flex items-center gap-1">
                            <span className="w-1 h-4 bg-red-500 block"></span>
                            Minimum add is ₹100
                        </p>
                    )}
                </div>

                {/* Yield Ticker */}
                <div className="bg-gray-50 dark:bg-gray-900 border-2 border-dashed border-gray-200 dark:border-gray-800 p-4 mb-6 space-y-3 relative mt-8">
                    <div className="absolute -top-3 left-3 bg-black text-white dark:bg-white dark:text-black px-2 py-1 text-[10px] font-black uppercase tracking-widest">
                        FEED CAPACITY
                    </div>
                    
                    <div className="flex justify-between items-center group/item hover:bg-white dark:hover:bg-black transition-colors p-2 -mx-2 rounded pt-2">
                        <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-white border border-black dark:border-none"></div>
                            <span className="font-bold text-sm uppercase">X (Twitter)</span>
                        </div>
                        <span className="font-mono font-black text-xl">
                            ~{Math.floor((topUpAmount || 0) / 1.5).toLocaleString()} <span className="text-xs text-gray-400">POSTS</span>
                        </span>
                    </div>
                    <div className="flex justify-between items-center group/item hover:bg-white dark:hover:bg-black transition-colors p-2 -mx-2 rounded">
                        <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-pink"></div>
                            <span className="font-bold text-sm uppercase">Instagram</span>
                        </div>
                        <span className="font-mono font-black text-xl">
                            ~{Math.floor((topUpAmount || 0) / 1.75).toLocaleString()} <span className="text-xs text-gray-400">POSTS</span>
                        </span>
                    </div>
                    <div className="flex justify-between items-center group/item hover:bg-white dark:hover:bg-black transition-colors p-2 -mx-2 rounded">
                        <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-red-500"></div>
                            <span className="font-bold text-sm uppercase">YouTube</span>
                        </div>
                        <span className="font-mono font-black text-xl">
                            ~{Math.floor((topUpAmount || 0) / 2.0).toLocaleString()} <span className="text-xs text-gray-400">POSTS</span>
                        </span>
                    </div>
                    <div className="flex justify-between items-center group/item hover:bg-white dark:hover:bg-black transition-colors p-2 -mx-2 rounded">
                        <div className="flex items-center gap-2">
                            <div className="w-2 h-2 rounded-full bg-blue-500"></div>
                            <span className="font-bold text-sm uppercase">LinkedIn</span>
                        </div>
                        <span className="font-mono font-black text-xl">
                            ~{Math.floor((topUpAmount || 0) / 2.25).toLocaleString()} <span className="text-xs text-gray-400">POSTS</span>
                        </span>
                    </div>
                </div>

                {/* Action Button */}
                <motion.button 
                    whileHover={{ scale: 1.02 }}
                    whileTap={{ scale: 0.98 }}
                    disabled={topUpAmount < 100 || isPaymentLoading}
                    onClick={handlePayment}
                    className="w-full py-4 bg-black dark:bg-white text-white dark:text-black font-black uppercase text-lg border-2 border-transparent hover:border-lime shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] hover:translate-x-[-2px] hover:translate-y-[-2px] hover:shadow-[6px_6px_0px_0px_#bef264] transition-all flex items-center justify-center gap-3 disabled:opacity-50 disabled:grayscale disabled:cursor-not-allowed disabled:hover:transform-none disabled:hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]"
                >
                    {isPaymentLoading ? (
                      <><Loader2 className="animate-spin" /> Processing...</>
                    ) : (
                      <>Add Funds <ArrowUpRight strokeWidth={3} /></>
                    )}
                </motion.button>
            </div>

            {/* Preferences (Unified Brutalist Toggles) */}
            <Card title="Preferences" className="mt-0">
                <div className="space-y-4">
                    {/* Email Notifications & Dark Mode - Unified Design */}
                    {[
                        { 
                            label: "Email Alerts", 
                            value: emailNotifications, 
                            action: async () => {
                                const newValue = !emailNotifications;
                                setEmailNotifications(newValue);
                                await getSupabase().from('users').update({ email_notifications: newValue }).eq('id', user?.id);
                            },
                            activeColor: "bg-lime"
                        },
                        { 
                            label: "Lights Out", 
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
                            activeColor: "bg-pink" 
                        }
                    ].map((item, index) => (
                        <div 
                            key={index}
                            onClick={item.action}
                            className="flex items-center justify-between p-4 border-4 border-black bg-white dark:bg-zinc-900 hover:translate-x-[2px] hover:translate-y-[2px] hover:shadow-none shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] transition-all cursor-pointer group select-none"
                        >
                            <span className="font-black uppercase text-sm tracking-wider flex items-center gap-3">
                                <div className={cn(
                                    "w-4 h-4 rounded-none border-2 border-black transition-colors",
                                    item.value ? item.activeColor : "bg-transparent"
                                )} />
                                {item.label}
                            </span>
                            <div className={cn(
                                "w-8 h-8 border-4 border-black flex items-center justify-center transition-colors bg-white",
                                item.value ? "bg-black" : "bg-white"
                            )}>
                                {item.value && <Check size={20} className="text-white" strokeWidth={5} />}
                            </div>
                        </div>
                    ))}
                </div>
            </Card>

            {/* HOTLINE (Control Panel Style) */}
            <div className="bg-zinc-100 dark:bg-zinc-900 border-4 border-black dark:border-white p-4 shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] flex flex-col gap-4 flex-grow">
                 <div className="flex items-center justify-between border-b-2 border-black dark:border-white pb-2 mb-2">
                     <div className="flex items-center gap-2">
                        <div className="w-3 h-3 bg-red-500 rounded-full animate-ping" />
                        <span className="font-black uppercase text-lg tracking-widest text-black dark:text-white">HOTLINE</span>
                     </div>
                 </div>

                 <div className="grid grid-cols-2 gap-4 flex-grow">
                    <a 
                        href="mailto:support@feedme.com?subject=SIGNAL_FAILURE" 
                        className="flex flex-col justify-center gap-3 bg-red-600 border-4 border-black dark:border-white p-4 hover:bg-red-500 hover:-translate-y-1 hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:hover:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] transition-all group min-h-[140px]"
                    >
                        <AlertOctagon className="text-white w-8 h-8 group-hover:rotate-12 transition-transform" strokeWidth={3} />
                        <div>
                            <span className="block font-black uppercase text-white text-xl leading-none mb-1">Signal Failure</span>
                            <span className="block font-bold uppercase text-red-900 text-[10px] tracking-wide">Report Bugs / Downtime</span>
                        </div>
                    </a>
                    
                    <a 
                        href="mailto:billing@feedme.com?subject=ORDER_INQUIRY" 
                        className="flex flex-col justify-center gap-3 bg-white dark:bg-black border-4 border-black dark:border-white p-4 hover:bg-gray-50 dark:hover:bg-zinc-900 hover:-translate-y-1 hover:shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] dark:hover:shadow-[4px_4px_0px_0px_rgba(255,255,255,1)] transition-all group min-h-[140px]"
                    >
                        <HelpCircle className="text-black dark:text-white w-8 h-8 group-hover:scale-110 transition-transform" strokeWidth={3} />
                        <div>
                            <span className="block font-black uppercase text-black dark:text-white text-xl leading-none mb-1">Order Inquiry</span>
                            <span className="block font-bold uppercase text-gray-500 dark:text-gray-400 text-[10px] tracking-wide">Billing Support</span>
                        </div>
                    </a>
                 </div>
            </div>

        </div>
      </div>
      {/* Payment Confirmation Modal - Brutalist Style */}
      <AnimatePresence>
        {isPaymentModalOpen && (
          <motion.div 
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/80 backdrop-blur-sm p-4"
          >
            <motion.div 
              initial={{ scale: 0.9, y: 20 }}
              animate={{ scale: 1, y: 0 }}
              exit={{ scale: 0.9, y: 20 }}
              className="bg-white dark:bg-zinc-900 border-4 border-lime w-full max-w-md shadow-[10px_10px_0px_0px_#bef264] relative"
            >
              <button 
                onClick={() => setIsPaymentModalOpen(false)}
                className="absolute top-2 right-2 p-2 hover:bg-gray-100 dark:hover:bg-black transition-colors"
              >
                  <div className="relative w-6 h-6">
                      <div className="absolute top-1/2 left-0 w-full h-0.5 bg-black dark:bg-white rotate-45 transform origin-center"></div>
                      <div className="absolute top-1/2 left-0 w-full h-0.5 bg-black dark:bg-white -rotate-45 transform origin-center"></div>
                  </div>
              </button>

              <div className="p-6">
                <div className="flex items-center gap-3 mb-6">
                   <div className="w-12 h-12 bg-lime border-2 border-black flex items-center justify-center">
                      <IndianRupee className="text-black w-6 h-6" />
                   </div>
                   <div>
                       <h3 className="text-2xl font-black uppercase italic tracking-tighter leading-none dark:text-white">
                           Protocol Check
                       </h3>
                       <p className="text-[10px] font-bold uppercase tracking-widest text-lime-600 dark:text-lime-400">
                           Manual Input Required
                       </p>
                   </div>
                </div>

                <div className="bg-gray-50 dark:bg-black border-2 border-dashed border-gray-300 dark:border-gray-700 p-4 mb-6 space-y-4 font-mono text-sm">
                   <div className="flex justify-between border-b border-gray-200 dark:border-gray-800 pb-2">
                       <span className="text-gray-500 uppercase font-bold">Protocol</span>
                       <span className="font-black dark:text-white">SECURE GATEWAY</span>
                   </div>
                   <div className="flex justify-between border-b border-gray-200 dark:border-gray-800 pb-2">
                       <span className="text-gray-500 uppercase font-bold">Amount</span>
                       <span className="font-black dark:text-white">₹{topUpAmount}</span>
                   </div>
                   <div className="flex justify-between pb-2">
                       <span className="text-gray-500 uppercase font-bold">Email</span>
                       <span className="font-black dark:text-white">{formData.email || user?.email}</span>
                   </div>
                </div>

                <div className="bg-yellow-50 dark:bg-yellow-900/20 border-l-4 border-yellow-400 p-3 mb-6">
                   <p className="text-xs font-bold text-yellow-800 dark:text-yellow-200 uppercase leading-relaxed">
                       <span className="font-black underline">ATTENTION:</span> Payment processor requires manual re-entry of Amount (₹{topUpAmount}) and Email to credit funds.
                   </p>
                </div>

                <div className="space-y-3">
                    <button 
                        onClick={handlePaymentLogic}
                        disabled={isPaymentLoading}
                        className="w-full py-4 bg-lime text-black font-black uppercase text-lg hover:bg-lime-400 transition-colors border-2 border-black shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] hover:translate-x-[2px] hover:translate-y-[2px] hover:shadow-none flex items-center justify-center gap-2"
                    >
                        {isPaymentLoading ? (
                            <><Loader2 className="animate-spin" /> ESTABLISHING LINK...</>
                        ) : (
                            <>ACKNOWLEDGE & PROCEED</>
                        )}
                    </button>
                    <button 
                        onClick={() => setIsPaymentModalOpen(false)}
                        className="w-full py-3 text-gray-400 font-bold uppercase text-xs hover:text-black dark:hover:text-white transition-colors"
                    >
                        Abort Mission
                    </button>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
      </div>
    </div>
  );
}
