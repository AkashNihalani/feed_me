'use client';

import { useState } from 'react';
import { useRouter } from 'next/navigation';
import { motion, AnimatePresence } from 'framer-motion';
import { Zap, Loader2, ArrowRight, UserPlus, KeyRound, LogIn, Eye, EyeOff } from 'lucide-react';
import { Input } from '@/components/ui/Input';
import BrutalistSlideshow from '@/components/ui/BrutalistSlideshow';
import { cn } from '@/lib/utils';
import { getSupabase } from '@/lib/supabase';

type AuthMode = 'login' | 'signup' | 'forgot';

export default function LoginPage() {
  const [mode, setMode] = useState<AuthMode>('login');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [isSuccess, setIsSuccess] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const router = useRouter();
  
  // Use the shared Supabase client for consistent auth state
  const supabase = getSupabase();

  const handleAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setMessage(null);

    try {
        if (mode === 'login') {
            const { error: signInError } = await supabase.auth.signInWithPassword({
                email,
                password,
            });
            if (signInError) throw signInError;
            
            // Success
            setIsSuccess(true);
            setTimeout(() => {
                router.push('/');
                router.refresh();
            }, 1000); // Faster transition
        } 
        else if (mode === 'signup') {
            const { error: signUpError } = await supabase.auth.signUp({
                email,
                password,
                options: {
                    emailRedirectTo: `${location.origin}/auth/callback`,
                },
            });
            if (signUpError) throw signUpError;
            setMessage('Account created! Please check your email to confirm.');
            setLoading(false);
        }
        else if (mode === 'forgot') {
            const { error: resetError } = await supabase.auth.resetPasswordForEmail(email, {
                redirectTo: `${location.origin}/auth/update-password`,
            });
            if (resetError) throw resetError;
            setMessage('Password reset link sent! Check your email.');
            setLoading(false);
        }
    } catch (err: any) {
        setError(err.message || 'An error occurred');
        setLoading(false);
    }
  };

  return (
    <div className="h-screen w-screen overflow-hidden bg-white text-black font-mono flex">
      
      {/* LEFT PANEL - Immersive Feature Rail (50%) */}
      {/* Hidden on Mobile, Block on Large Screens */}
      <div className="hidden lg:block w-1/2 h-full relative z-0 border-r-8 border-black">
         <BrutalistSlideshow />
      </div>

      {/* RIGHT PANEL - Auth Form (50%) */}
      <div className="w-full lg:w-1/2 h-full flex items-center justify-center p-12 bg-white relative">
         
         <style jsx global>{`
           input:-webkit-autofill,
           input:-webkit-autofill:hover, 
           input:-webkit-autofill:focus, 
           input:-webkit-autofill:active {
               -webkit-box-shadow: 0 0 0 30px white inset !important;
               -webkit-text-fill-color: black !important;
               caret-color: black !important;
           }
         `}</style>
         
         {/* Brutalist Decorative Elements */}
         <div className="absolute top-0 right-0 p-8">
             <div className="w-16 h-16 border-t-8 border-r-8 border-black"></div>
         </div>
         <div className="absolute bottom-0 left-0 p-8">
             <div className="w-16 h-16 border-b-8 border-l-8 border-black"></div>
         </div>
         
         {/* Grid Pattern (Subtle) */}
         <div className="absolute inset-0 bg-[linear-gradient(to_right,#00000008_1px,transparent_1px),linear-gradient(to_bottom,#00000008_1px,transparent_1px)] bg-[size:40px_40px] pointer-events-none"></div>

         <div className="w-full max-w-lg relative z-10">
             
             {/* Header */}
             <div className="mb-12">
                <div className="inline-flex items-center gap-1 mb-6 transform hover:scale-105 transition-transform origin-left">
                    <span className="font-black text-5xl italic tracking-tighter bg-black text-white px-4 py-2 border-4 border-black shadow-[8px_8px_0px_0px_rgba(0,0,0,0.2)]">FEED</span>
                    <span className="font-black text-5xl italic tracking-tighter bg-lime text-black px-4 py-2 border-4 border-black -ml-2">ME</span>
                </div>
                <h1 className="text-6xl font-black uppercase tracking-tighter mb-4 leading-none text-black">
                    {mode === 'login' && 'Terminal\nAccess.'}
                    {mode === 'signup' && 'Initialize\nSequence.'}
                    {mode === 'forgot' && 'Reset\nProtocol.'}
                </h1>
                <p className="font-bold uppercase tracking-wider text-sm bg-black text-white inline-block px-3 py-1">
                    {mode === 'login' && 'Enter coordinates to proceed.'}
                    {mode === 'signup' && 'Start aggregating your digital footprint.'}
                    {mode === 'forgot' && 'Secure link will be deployed.'}
                </p>
             </div>

             {/* Mode Toggles */}
             <div className="flex mb-10 gap-6">
                 <button 
                   onClick={() => setMode('login')}
                   className={cn(
                       "text-2xl font-black uppercase transition-all hover:-translate-y-1",
                       mode === 'login' ? "underline decoration-8 decoration-lime underline-offset-4 text-black" : "text-gray-300 hover:text-gray-400"
                   )}
                 >Login</button>
                 <button 
                   onClick={() => setMode('signup')}
                   className={cn(
                       "text-2xl font-black uppercase transition-all hover:-translate-y-1",
                       mode === 'signup' ? "underline decoration-8 decoration-pink underline-offset-4 text-black" : "text-gray-300 hover:text-gray-400"
                   )}
                 >Sign Up</button>
             </div>

             {/* Form */}
             <form onSubmit={handleAuth} className="space-y-8">
                 <AnimatePresence mode="wait">
                     <motion.div
                        key={mode}
                        initial={{ opacity: 0, x: 20 }}
                        animate={{ opacity: 1, x: 0 }}
                        exit={{ opacity: 0, x: -20 }}
                        transition={{ duration: 0.2 }}
                     >
                        <div className="space-y-6">
                            <div className="group">
                                <label className="block text-sm font-black uppercase mb-2 text-black transition-colors">Identity (Email)</label>
                                <Input 
                                type="email" 
                                value={email}
                                onChange={(e) => setEmail(e.target.value)}
                                required
                                placeholder="USER@DOMAIN.COM"
                                className="h-20 border-4 border-black !bg-white !text-black font-black text-2xl focus:ring-0 focus:border-black focus:shadow-[8px_8px_0px_0px_#000000] rounded-none placeholder:text-gray-300 px-6 transition-all"
                                />
                            </div>

                            {mode !== 'forgot' && (
                                <div className="group relative">
                                    <div className="flex justify-between items-center mb-2">
                                        <label className="block text-sm font-black uppercase text-black transition-colors">Passcode</label>
                                        {mode === 'login' && (
                                            <button 
                                              type="button" 
                                              onClick={() => setMode('forgot')}
                                              className="text-xs font-black uppercase text-gray-500 hover:text-black border-b-2 border-transparent hover:border-black transition-all"
                                            >
                                                Forgot?
                                            </button>
                                        )}
                                    </div>
                                    <div className="relative">
                                        <Input 
                                        type={showPassword ? "text" : "password"}
                                        value={password}
                                        onChange={(e) => setPassword(e.target.value)}
                                        required
                                        placeholder="••••••••"
                                        className="h-20 border-4 border-black !bg-white !text-black font-black text-2xl focus:ring-0 focus:border-black focus:shadow-[8px_8px_0px_0px_#000000] rounded-none placeholder:text-gray-300 px-6 pr-16 transition-all"
                                        />
                                        <button
                                            type="button"
                                            onClick={() => setShowPassword(!showPassword)}
                                            className="absolute right-4 top-1/2 -translate-y-1/2 p-2 hover:bg-gray-100 rounded-none transition-colors"
                                        >
                                            {showPassword ? (
                                                <EyeOff size={24} strokeWidth={3} className="text-black" />
                                            ) : (
                                                <Eye size={24} strokeWidth={3} className="text-black" />
                                            )}
                                        </button>
                                    </div>
                                </div>
                            )}
                        </div>
                     </motion.div>
                 </AnimatePresence>

                 {error && (
                    <div className="bg-red-500 text-white p-4 font-black text-sm uppercase border-4 border-black shadow-[4px_4px_0px_0px_#000]">
                        ⚠ {error}
                    </div>
                 )}

                 {message && (
                    <div className="bg-lime text-black p-4 font-black text-sm uppercase border-4 border-black shadow-[4px_4px_0px_0px_#000]">
                        ✓ {message}
                    </div>
                 )}

                 <button 
                   type="submit"
                   disabled={loading || isSuccess}
                   className={cn(
                       "w-full h-20 font-black text-3xl uppercase tracking-wider border-4 border-black transition-all flex items-center justify-center gap-4 disabled:opacity-50 disabled:cursor-not-allowed shadow-[8px_8px_0px_0px_#000] hover:translate-y-1 hover:shadow-none active:translate-y-2 rounded-none",
                       mode === 'signup' 
                         ? "bg-pink text-white hover:bg-pink-600"  
                         : "bg-black text-white hover:bg-gray-900" 
                   )}
                 >
                     {loading ? (
                         <Loader2 className="animate-spin w-8 h-8" />
                     ) : (
                         <>
                             {mode === 'login' && <>ENTER <LogIn strokeWidth={3} size={32} /></>}
                             {mode === 'signup' && <>JOIN <UserPlus strokeWidth={3} size={32} /></>}
                             {mode === 'forgot' && <>SEND <KeyRound strokeWidth={3} size={32} /></>}
                         </>
                     )}
                 </button>
             </form>
         </div>
      </div>

      {/* Success Transition Overlay */}
      <AnimatePresence>
        {isSuccess && (
            <motion.div
                initial={{ x: '100%' }}
                animate={{ x: 0 }}
                exit={{ x: '-100%' }}
                transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }} 
                className="fixed inset-0 z-50 bg-lime flex items-center justify-center pointer-events-none"
            >
                <motion.div
                    initial={{ scale: 0.8, opacity: 0 }}
                    animate={{ scale: 1, opacity: 1 }}
                    transition={{ delay: 0.2, duration: 0.4 }}
                    className="text-center"
                >
                    <h1 className="text-9xl font-black italic tracking-tighter text-black mb-4">
                        ACCESS<br/>GRANTED
                    </h1>
                    <div className="h-4 bg-black w-full animate-pulse"></div>
                </motion.div>
            </motion.div>
        )}
      </AnimatePresence>

    </div>
  );
}
