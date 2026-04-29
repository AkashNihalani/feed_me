'use client';

import { useEffect, useMemo, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { ArrowLeft, ArrowRight, RotateCcw, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { GRID_LAYOUT_SPRING } from '@/lib/motion';
import {
  FocusBrief,
  RELATIONSHIP_OPTIONS,
  ACCOUNT_TYPE_OPTIONS,
  CATEGORY_OPTIONS,
  GEOGRAPHY_OPTIONS,
  PRIORITY_OPTIONS,
  CREATE_FEED_STEPS,
  CREATE_FEED_LAST_STEP,
  MAX_PRIORITY_SELECTIONS,
  buildFocusBible,
} from './focusUtils';

type FocusDialogProps = {
  open: boolean;
  mode: 'create' | 'edit';
  title: string;
  feedName?: string;
  brief: FocusBrief;
  step: number;
  bibleDraft: string;
  isBusy: boolean;
  onFeedNameChange?: (value: string) => void;
  onBriefChange: (patch: Partial<FocusBrief>) => void;
  onStepChange: (step: number) => void;
  onBibleDraftChange: (value: string) => void;
  onClose: () => void;
  onPrimary: () => void;
  onBack: () => void;
  onStartOver?: () => void;
};

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;

export default function FocusDialog({
  open,
  mode,
  title,
  feedName = '',
  brief,
  step,
  bibleDraft,
  isBusy,
  onFeedNameChange,
  onBriefChange,
  onStepChange,
  onBibleDraftChange,
  onClose,
  onPrimary,
  onBack,
  onStartOver,
}: FocusDialogProps) {
  const generatedSummary = useMemo(() => buildFocusBible(brief, title || feedName || 'this feed'), [brief, feedName, title]);
  const [typedSummary, setTypedSummary] = useState('');
  const [isTypingSummary, setIsTypingSummary] = useState(false);
  const isSummaryStep = step === CREATE_FEED_LAST_STEP;
  const canProceed = mode === 'edit' || feedName.trim().length > 0;
  const progress = `${Math.max(0, Math.min(100, (step / CREATE_FEED_LAST_STEP) * 100))}%`;
  const selectedAccountTypes = brief.accountTypes.length > 1 ? brief.accountTypes : brief.accountTypes.filter((value) => value !== 'Mixed');
  const summaryValue = isSummaryStep && isTypingSummary && !bibleDraft.trim() ? typedSummary : bibleDraft;

  useEffect(() => {
    if (!open) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [onClose, open]);

  useEffect(() => {
    if (!open || !isSummaryStep || bibleDraft.trim()) {
      return undefined;
    }
    const text = generatedSummary;
    const stepSize = Math.max(1, Math.ceil(text.length / 50));
    const delay = Math.max(12, Math.min(40, Math.floor(1500 / Math.max(text.length / stepSize, 1))));
    let index = 0;
    let timeoutId: number | undefined;

    const tick = () => {
      index = Math.min(text.length, index + stepSize);
      setTypedSummary(text.slice(0, index));
      if (index >= text.length) {
        setIsTypingSummary(false);
        onBibleDraftChange(text);
        return;
      }
      timeoutId = window.setTimeout(tick, delay);
    };

    timeoutId = window.setTimeout(() => {
      setIsTypingSummary(true);
      setTypedSummary('');
      tick();
    }, 0);
    return () => {
      if (timeoutId != null) window.clearTimeout(timeoutId);
    };
  }, [bibleDraft, generatedSummary, isSummaryStep, onBibleDraftChange, open]);

  const toggleAccountType = (option: string) => {
    const exists = selectedAccountTypes.includes(option);
    const next = exists
      ? selectedAccountTypes.filter((value) => value !== option)
      : [...selectedAccountTypes, option];
    onBriefChange({ accountTypes: next.length > 0 ? next : [option] });
  };

  const togglePriority = (option: string) => {
    const exists = brief.priorities.includes(option);
    if (exists) {
      onBriefChange({ priorities: brief.priorities.filter((value) => value !== option) });
      return;
    }
    if (brief.priorities.length >= MAX_PRIORITY_SELECTIONS) return;
    onBriefChange({ priorities: [...brief.priorities, option] });
  };

  const renderChip = (
    label: string,
    active: boolean,
    onClick: () => void,
    description?: string,
    disabled = false,
  ) => (
    <motion.button
      key={label}
      layout
      type="button"
      whileTap={{ scale: disabled ? 1 : 0.96 }}
      onClick={onClick}
      disabled={disabled}
      className={cn(
        'relative overflow-hidden rounded-[16px] border px-3.5 py-2.5 text-left text-[13px] font-black tracking-normal transition disabled:opacity-35',
        active
          ? 'border-transparent text-white dark:text-white'
          : 'border-black/10 bg-black/[0.03] text-neutral-600 hover:bg-black/[0.06] dark:border-white/8 dark:bg-white/[0.035] dark:text-white/62 dark:hover:bg-white/[0.07]',
      )}
    >
      {active && (
        <motion.div
          layoutId={`active-chip-bg-${step}-${label}`}
          className="absolute inset-0 z-0 bg-neutral-900 dark:bg-white/14"
          transition={GRID_LAYOUT_SPRING}
          style={{ borderRadius: 16 }}
        />
      )}
      <span className="relative z-10 block">{label}</span>
      {description && <span className={cn("relative z-10 mt-1 block text-[11px] font-semibold leading-snug", active ? "text-white/70 dark:text-white/70" : "text-neutral-400 dark:text-white/42")}>{description}</span>}
    </motion.button>
  );

  return (
    <AnimatePresence>
      {open && (
        <motion.div
          className="fixed inset-0 z-[260] flex items-end justify-center px-0 md:items-center md:px-8 md:py-8"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
        >
          {/* Backdrop */}
          <motion.div
            className="absolute inset-0 bg-black/40 backdrop-blur-sm dark:bg-black/60"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.22 }}
            onClick={onClose}
          />
          <motion.div
            drag="y"
            dragConstraints={{ top: 0, bottom: 0 }}
            dragElastic={{ top: 0, bottom: 0.22 }}
            onDragEnd={(_, info) => {
              if (info.offset.y > 120) onClose();
            }}
            initial={{ y: '100%', scale: 0.98 }}
            animate={{ y: 0, scale: 1 }}
            exit={{ y: '100%', scale: 0.98 }}
            transition={GRID_LAYOUT_SPRING}
            className="relative flex h-[calc(100dvh-env(safe-area-inset-top))] w-full flex-col overflow-hidden rounded-t-[32px] border border-black/10 bg-white/80 text-black shadow-[0_-12px_40px_rgba(0,0,0,0.12)] backdrop-blur-2xl md:h-auto md:max-h-[min(700px,calc(100vh-4rem))] md:w-[540px] md:rounded-[32px] md:border-black/10 md:shadow-[0_24px_60px_rgba(0,0,0,0.16)] dark:border-white/10 dark:bg-[#07080a]/80 dark:text-white dark:shadow-[0_-12px_40px_rgba(0,0,0,0.4)] md:dark:shadow-[0_24px_60px_rgba(0,0,0,0.6)]"
          >
            <div className="mx-auto mt-3 h-1.5 w-12 rounded-full bg-black/10 dark:bg-white/18 md:hidden" />
            <div className="border-b border-black/10 px-5 pb-4 pt-4 dark:border-white/8 md:px-6 md:pt-6">
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="text-[10px] font-black uppercase tracking-[0.18em] text-neutral-400 dark:text-white/38">Focus</div>
                  <div className="mt-1 truncate text-[26px] font-black tracking-normal">{mode === 'create' ? 'New feed' : title}</div>
                  <p className="mt-2 text-[14px] font-semibold leading-relaxed text-neutral-500 dark:text-white/52">Two minutes. We&apos;ll use this to read every signal in your voice.</p>
                </div>
                <button type="button" onClick={onClose} className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full border border-black/10 bg-black/[0.03] text-neutral-500 dark:border-white/8 dark:bg-white/[0.035] dark:text-white/58">
                  <X size={17} strokeWidth={2.4} />
                </button>
              </div>
              {mode === 'create' && (
                <label className="mt-4 block">
                  <span className="text-[10px] font-black uppercase tracking-[0.16em] text-neutral-400 dark:text-white/36">Feed name</span>
                  <input
                    maxLength={15}
                    value={feedName}
                    onChange={(event) => onFeedNameChange?.(event.target.value.toUpperCase())}
                    placeholder="BEAUTY"
                    className="mt-2 w-full rounded-[14px] border border-black/10 bg-black/5 px-4 py-3 text-[20px] font-black uppercase tracking-normal text-black outline-none placeholder:text-black/30 dark:border-white/8 dark:bg-white/[0.04] dark:text-white dark:placeholder:text-white/22"
                  />
                </label>
              )}
              <div className="mt-5">
                <div className="h-1 rounded-full bg-black/10 dark:bg-white/8">
                  <motion.div className="h-full rounded-full bg-[#E11D48]" animate={{ width: progress }} transition={{ duration: 0.2, ease: APPLE_EASE }} />
                </div>
                <div className="mt-3 flex items-center justify-between">
                  {CREATE_FEED_STEPS.map((label, index) => {
                    const isActive = index === step;
                    const isPast = index < step;
                    return (
                      <button key={label} type="button" onClick={() => onStepChange(index)} className="group relative flex flex-col items-center gap-1">
                        <div className="relative flex h-3 w-3 items-center justify-center">
                          {isActive && (
                            <motion.div
                              layoutId="active-step-indicator"
                              className="absolute inset-0 rounded-full bg-[#E11D48]"
                              transition={GRID_LAYOUT_SPRING}
                            />
                          )}
                          <span className={cn('relative z-10 block h-2.5 w-2.5 rounded-full border transition', isActive ? 'border-transparent' : isPast ? 'border-black/50 bg-black/40 dark:border-white/50 dark:bg-white/40' : 'border-black/16 bg-black/6 dark:border-white/16 dark:bg-white/6')} />
                        </div>
                        <span className={cn("hidden text-[9px] font-black uppercase tracking-[0.12em] transition sm:block md:hidden lg:block", isActive ? "text-[#E11D48]" : isPast ? "text-neutral-500 dark:text-white/50" : "text-neutral-400 dark:text-white/32")}>{label}</span>
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>

            <div className="min-h-0 flex-1 overflow-hidden px-5 py-5 md:px-6">
              <AnimatePresence mode="wait" initial={false}>
                {step === 0 && (
                  <motion.div key="why" initial={{ opacity: 0, scale: 0.96 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.96 }} transition={GRID_LAYOUT_SPRING} className="flex h-full flex-col">
                    <div>
                      <div className="text-[22px] font-black tracking-normal">Why are you tracking this?</div>
                    </div>
                    <div className="mt-5 grid gap-3 overflow-y-auto pr-1">
                      {RELATIONSHIP_OPTIONS.map((option) => renderChip(
                        option.label,
                        brief.relationship === option.label,
                        () => onBriefChange({ relationship: option.label }),
                        option.description,
                      ))}
                    </div>
                  </motion.div>
                )}

                {step === 1 && (
                  <motion.div key="who" initial={{ opacity: 0, scale: 0.96 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.96 }} transition={GRID_LAYOUT_SPRING} className="flex h-full flex-col overflow-y-auto pr-1">
                    <div className="text-[22px] font-black tracking-normal">Who are you tracking?</div>
                    <div className="mt-5">
                      <div className="text-[10px] font-black uppercase tracking-[0.16em] text-neutral-400 dark:text-white/36">Account types</div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {ACCOUNT_TYPE_OPTIONS.map((option) => renderChip(option, selectedAccountTypes.includes(option), () => toggleAccountType(option)))}
                      </div>
                    </div>
                    <div className="mt-5">
                      <div className="text-[10px] font-black uppercase tracking-[0.16em] text-neutral-400 dark:text-white/36">Category</div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {CATEGORY_OPTIONS.map((option) => renderChip(option, brief.category === option, () => onBriefChange({ category: option })))}
                      </div>
                      <input
                        value={brief.category}
                        onChange={(event) => onBriefChange({ category: event.target.value })}
                        placeholder="e.g., premium bakeries, indie skincare, football creators"
                        className="mt-3 w-full rounded-[14px] border border-black/10 bg-black/5 px-4 py-3 text-[15px] font-semibold text-black outline-none placeholder:text-black/30 dark:border-white/8 dark:bg-white/[0.04] dark:text-white dark:placeholder:text-white/24"
                      />
                    </div>
                    <div className="mt-5">
                      <div className="text-[10px] font-black uppercase tracking-[0.16em] text-neutral-400 dark:text-white/36">Where</div>
                      <div className="mt-2 flex flex-wrap gap-2">
                        {GEOGRAPHY_OPTIONS.map((option) => renderChip(option, brief.geography === option, () => onBriefChange({ geography: option })))}
                      </div>
                      <input
                        value={brief.geography}
                        onChange={(event) => onBriefChange({ geography: event.target.value })}
                        placeholder="Global, country, or city - wherever the audience lives"
                        className="mt-3 w-full rounded-[14px] border border-black/10 bg-black/5 px-4 py-3 text-[15px] font-semibold text-black outline-none placeholder:text-black/30 dark:border-white/8 dark:bg-white/[0.04] dark:text-white dark:placeholder:text-white/24"
                      />
                    </div>
                  </motion.div>
                )}

                {step === 2 && (
                  <motion.div key="attention" initial={{ opacity: 0, scale: 0.96 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.96 }} transition={GRID_LAYOUT_SPRING} className="flex h-full flex-col">
                    <div className="text-[22px] font-black tracking-normal">Whose attention matters here?</div>
                    <p className="mt-3 text-[15px] font-semibold leading-relaxed text-neutral-500 dark:text-white/52">The people we&apos;re trying to understand - buyers, fans, viewers, hiring managers, dessert lovers.</p>
                    <input
                      value={brief.audience}
                      onChange={(event) => onBriefChange({ audience: event.target.value })}
                      placeholder="Young urban dessert buyers in Mumbai. Gen Z skincare shoppers. Indie game devs."
                      className="mt-6 w-full rounded-[16px] border border-black/10 bg-black/5 px-4 py-4 text-[16px] font-semibold leading-relaxed text-black outline-none placeholder:text-black/30 dark:border-white/8 dark:bg-white/[0.04] dark:text-white dark:placeholder:text-white/24"
                    />
                  </motion.div>
                )}

                {step === 3 && (
                  <motion.div key="first" initial={{ opacity: 0, scale: 0.96 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.96 }} transition={GRID_LAYOUT_SPRING} className="flex h-full flex-col">
                    <div className="text-[22px] font-black tracking-normal">What do you want to see first?</div>
                    <p className="mt-3 text-[15px] font-semibold leading-relaxed text-neutral-500 dark:text-white/52">Pick up to three. These sort cards higher; they never hide real signals.</p>
                    <div className="mt-5 flex flex-wrap gap-2 overflow-y-auto pr-1">
                      {PRIORITY_OPTIONS.map((option) => {
                        const active = brief.priorities.includes(option);
                        const disabled = !active && brief.priorities.length >= MAX_PRIORITY_SELECTIONS;
                        return renderChip(option, active, () => togglePriority(option), undefined, disabled);
                      })}
                    </div>
                  </motion.div>
                )}

                {step === 4 && (
                  <motion.div key="context" initial={{ opacity: 0, scale: 0.96 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.96 }} transition={GRID_LAYOUT_SPRING} className="flex h-full flex-col">
                    <div className="text-[22px] font-black tracking-normal">Anything else we should know?</div>
                    <p className="mt-3 text-[15px] font-semibold leading-relaxed text-neutral-500 dark:text-white/52">Tell us about the world we&apos;re entering. This is optional.</p>
                    <textarea
                      value={brief.note}
                      onChange={(event) => onBriefChange({ note: event.target.value.slice(0, 280) })}
                      placeholder="Local slang, recurring campaigns, anchor accounts of the audience's attention, anything Feed Me wouldn't know from the handles alone."
                      className="mt-6 min-h-[190px] w-full resize-none rounded-[16px] border border-black/10 bg-black/5 px-4 py-4 text-[15px] font-semibold leading-relaxed text-black outline-none placeholder:text-black/30 dark:border-white/8 dark:bg-white/[0.04] dark:text-white dark:placeholder:text-white/24"
                    />
                    <div className="mt-2 text-right text-[11px] font-black uppercase tracking-[0.12em] text-neutral-400 dark:text-white/30">{brief.note.length} / 280</div>
                  </motion.div>
                )}

                {isSummaryStep && (
                  <motion.div key="brain" initial={{ opacity: 0, scale: 0.96 }} animate={{ opacity: 1, scale: 1 }} exit={{ opacity: 0, scale: 0.96 }} transition={GRID_LAYOUT_SPRING} className="flex h-full flex-col">
                    <div className="flex items-start justify-between gap-3">
                      <div>
                        <div className="text-[22px] font-black tracking-normal">Your Focus</div>
                        <p className="mt-3 text-[15px] font-semibold leading-relaxed text-neutral-500 dark:text-white/52">Edit the summary until it sounds like the way you want this feed read.</p>
                      </div>
                      <button
                        type="button"
                        onClick={() => onBibleDraftChange('')}
                        className="shrink-0 rounded-[14px] border border-black/10 bg-black/[0.03] px-3 py-2 text-[11px] font-black text-neutral-500 dark:border-white/8 dark:bg-white/[0.04] dark:text-white/58"
                      >
                        Generate from inputs
                      </button>
                    </div>
                    <textarea
                      value={summaryValue}
                      onChange={(event) => {
                        setIsTypingSummary(false);
                        onBibleDraftChange(event.target.value.slice(0, 700));
                      }}
                      className="mt-5 min-h-[260px] flex-1 resize-none rounded-[18px] border border-black/10 bg-black/5 px-4 py-4 text-[16px] font-semibold leading-relaxed text-black outline-none placeholder:text-black/30 dark:border-white/8 dark:bg-white/[0.04] dark:text-white dark:placeholder:text-white/24"
                    />
                  </motion.div>
                )}
              </AnimatePresence>
            </div>

            <div className="border-t border-black/10 bg-black/5 px-5 pb-[calc(16px+env(safe-area-inset-bottom))] pt-4 dark:border-white/8 dark:bg-[#07080a] md:px-6">
              <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  {step > 0 && (
                    <button type="button" onClick={onBack} className="flex h-11 items-center gap-2 rounded-[14px] border border-black/10 bg-white/50 px-4 text-[12px] font-black text-neutral-600 dark:border-white/8 dark:bg-white/[0.035] dark:text-white/64">
                      <ArrowLeft size={15} strokeWidth={2.5} /> Back
                    </button>
                  )}
                  {isSummaryStep && onStartOver && (
                    <button type="button" onClick={onStartOver} className="hidden h-11 items-center gap-2 rounded-[14px] border border-black/10 bg-white/50 px-4 text-[12px] font-black text-neutral-400 dark:border-white/8 dark:bg-white/[0.035] dark:text-white/46 sm:flex">
                      <RotateCcw size={14} strokeWidth={2.5} /> Start over
                    </button>
                  )}
                </div>
                <motion.button
                  type="button"
                  whileTap={{ scale: 0.96 }}
                  animate={isSummaryStep ? { scale: [1, 1.025, 1] } : { scale: 1 }}
                  transition={isSummaryStep ? { duration: 0.42, ease: APPLE_EASE } : undefined}
                  onClick={onPrimary}
                  disabled={!canProceed || isBusy}
                  className="flex h-11 items-center gap-2 rounded-[14px] bg-[#E11D48] px-5 text-[12px] font-black text-white disabled:opacity-40"
                >
                  {isBusy ? (isSummaryStep ? 'Saving' : 'Working') : isSummaryStep ? 'Save Focus' : 'Next'}
                  {!isBusy && !isSummaryStep && <ArrowRight size={15} strokeWidth={2.6} />}
                </motion.button>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
