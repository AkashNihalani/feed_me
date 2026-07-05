'use client';

import { useEffect, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { Check, Download, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import { GRID_LAYOUT_SPRING } from '@/lib/motion';
import {
  ALL_EXPORT_FIELD_IDS,
  DEFAULT_EXPORT_FIELD_IDS,
  EXPORT_FIELD_GROUPS,
  ExportFieldId,
  MINIMAL_EXPORT_FIELD_IDS,
} from '@/lib/feedExportConfig';

type FeedExportDialogProps = {
  open: boolean;
  scopeLabel: string;
  from: string;
  to: string;
  fields: ExportFieldId[];
  includeSummary: boolean;
  isExporting: boolean;
  error: string | null;
  onFromChange: (value: string) => void;
  onToChange: (value: string) => void;
  onFieldsChange: (fields: ExportFieldId[]) => void;
  onIncludeSummaryChange: (value: boolean) => void;
  onClose: () => void;
  onExport: () => void;
};

const PRESETS = [
  { id: 'recommended', label: 'Recommended', fields: DEFAULT_EXPORT_FIELD_IDS },
  { id: 'minimal', label: 'Minimal', fields: MINIMAL_EXPORT_FIELD_IDS },
  { id: 'all', label: 'All Fields', fields: ALL_EXPORT_FIELD_IDS },
] as const;

export default function FeedExportDialog({
  open,
  scopeLabel,
  from,
  to,
  fields,
  includeSummary,
  isExporting,
  error,
  onFromChange,
  onToChange,
  onFieldsChange,
  onIncludeSummaryChange,
  onClose,
  onExport,
}: FeedExportDialogProps) {
  const selected = useMemo(() => new Set(fields), [fields]);
  const mounted = typeof document !== 'undefined';
  const hasFields = fields.length > 0;
  const hasInvalidRange = Boolean(from && to && from > to);

  useEffect(() => {
    if (!open || typeof window === 'undefined') return undefined;
    const previousOverflow = document.documentElement.style.overflow;
    document.documentElement.style.overflow = 'hidden';
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.documentElement.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [onClose, open]);

  const setPreset = (presetFields: readonly ExportFieldId[]) => {
    onFieldsChange([...presetFields]);
  };

  const toggleField = (field: ExportFieldId) => {
    const next = new Set(selected);
    if (next.has(field)) next.delete(field);
    else next.add(field);
    onFieldsChange(ALL_EXPORT_FIELD_IDS.filter((id) => next.has(id)));
  };

  const toggleGroup = (groupFields: readonly { id: ExportFieldId; label: string }[]) => {
    const next = new Set(selected);
    const allSelected = groupFields.every((field) => next.has(field.id));
    groupFields.forEach((field) => {
      if (allSelected) next.delete(field.id);
      else next.add(field.id);
    });
    onFieldsChange(ALL_EXPORT_FIELD_IDS.filter((id) => next.has(id)));
  };

  const dialog = (
    <AnimatePresence>
      {open && (
        <motion.div
          className="fixed inset-0 z-[300] flex items-end justify-center px-0 md:items-center md:px-6 md:py-6"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
        >
          <motion.div
            className="absolute inset-0 bg-black/58 dark:bg-black/74"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            onClick={isExporting ? undefined : onClose}
          />

          <motion.div
            role="dialog"
            aria-modal="true"
            aria-label="Export workbook"
            drag="y"
            dragConstraints={{ top: 0, bottom: 0 }}
            dragElastic={{ top: 0, bottom: isExporting ? 0 : 0.2 }}
            onDragEnd={(_, info) => {
              if (!isExporting && info.offset.y > 120) onClose();
            }}
            initial={{ y: '100%', scale: 0.98 }}
            animate={{ y: 0, scale: 1 }}
            exit={{ y: '100%', scale: 0.98 }}
            transition={GRID_LAYOUT_SPRING}
            onClick={(event) => event.stopPropagation()}
            className="relative flex h-[calc(100dvh-env(safe-area-inset-top))] w-full flex-col overflow-hidden rounded-t-[28px] border border-black/10 bg-white/96 text-black shadow-[0_-18px_60px_rgba(0,0,0,0.18)] md:h-auto md:max-h-[min(760px,calc(100vh-3rem))] md:w-[min(680px,calc(100vw-48px))] md:rounded-[28px] dark:border-white/10 dark:bg-[var(--fm-ink)]/96 dark:text-white dark:shadow-[0_30px_90px_rgba(0,0,0,0.64)]"
          >
            <div className="mx-auto mt-3 h-1.5 w-12 rounded-full bg-black/10 dark:bg-white/18 md:hidden" />

            <div className="shrink-0 border-b border-black/10 px-5 pb-4 pt-4 dark:border-white/8 md:px-6 md:pt-6">
              <div className="flex items-start justify-between gap-4">
                <div className="min-w-0">
                  <div className="text-[10px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent)]">Excel Export</div>
                  <div className="mt-1 truncate text-[28px] font-black tracking-normal">Feed workbook</div>
                  <div className="mt-2 inline-flex max-w-full items-center rounded-full border border-black/10 bg-black/[0.035] px-3 py-1 text-[10px] font-black uppercase tracking-[0.14em] text-black/48 dark:border-white/8 dark:bg-white/[0.05] dark:text-white/42">
                    <span className="truncate">{scopeLabel}</span>
                  </div>
                </div>
                <button
                  type="button"
                  onClick={onClose}
                  disabled={isExporting}
                  className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full border border-black/10 bg-black/[0.03] text-neutral-500 transition hover:bg-black/[0.06] disabled:opacity-45 dark:border-white/8 dark:bg-white/[0.035] dark:text-white/58 dark:hover:bg-white/[0.07]"
                  aria-label="Close export dialog"
                >
                  <X size={17} strokeWidth={2.4} />
                </button>
              </div>
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto px-5 py-4 md:px-6">
              <div className="grid grid-cols-2 gap-2.5">
                <label className="block rounded-[18px] border border-black/10 bg-black/[0.025] px-3 py-2.5 dark:border-white/8 dark:bg-white/[0.035]">
                  <span className="text-[10px] font-black uppercase tracking-[0.14em] text-black/42 dark:text-white/36">From</span>
                  <input
                    type="date"
                    value={from}
                    max={to || undefined}
                    onChange={(event) => onFromChange(event.target.value)}
                    className="mt-1 w-full bg-transparent text-[16px] font-black text-black outline-none dark:text-white"
                  />
                </label>
                <label className="block rounded-[18px] border border-black/10 bg-black/[0.025] px-3 py-2.5 dark:border-white/8 dark:bg-white/[0.035]">
                  <span className="text-[10px] font-black uppercase tracking-[0.14em] text-black/42 dark:text-white/36">To</span>
                  <input
                    type="date"
                    value={to}
                    min={from || undefined}
                    onChange={(event) => onToChange(event.target.value)}
                    className="mt-1 w-full bg-transparent text-[16px] font-black text-black outline-none dark:text-white"
                  />
                </label>
              </div>

              <div className="mt-4 grid grid-cols-3 gap-2">
                {PRESETS.map((preset) => (
                  <button
                    key={preset.id}
                    type="button"
                    onClick={() => setPreset(preset.fields)}
                    className="rounded-[14px] border border-black/10 bg-black/[0.025] px-2.5 py-2 text-[10px] font-black uppercase tracking-[0.14em] text-black/54 transition hover:bg-black/[0.06] dark:border-white/8 dark:bg-white/[0.035] dark:text-white/46 dark:hover:bg-white/[0.07]"
                  >
                    {preset.label}
                  </button>
                ))}
              </div>

              <button
                type="button"
                onClick={() => onIncludeSummaryChange(!includeSummary)}
                className={cn(
                  'mt-4 flex w-full items-center justify-between gap-3 rounded-[18px] border px-3.5 py-3 text-left transition',
                  includeSummary
                    ? 'border-[var(--fm-accent)]/30 bg-[var(--fm-accent)]/10 text-black dark:text-white'
                    : 'border-black/10 bg-black/[0.025] text-black/58 dark:border-white/8 dark:bg-white/[0.035] dark:text-white/46',
                )}
              >
                <span className="text-[12px] font-black uppercase tracking-[0.14em]">Summary sheet</span>
                <span className={cn(
                  'flex h-5 w-5 items-center justify-center rounded-full border',
                  includeSummary ? 'border-[var(--fm-accent)] bg-[var(--fm-accent)] text-white' : 'border-black/14 dark:border-white/14',
                )}>
                  {includeSummary && <Check size={13} strokeWidth={3} />}
                </span>
              </button>

              <div className="mt-5 space-y-3">
                {EXPORT_FIELD_GROUPS.map((group) => {
                  const groupSelected = group.fields.filter((field) => selected.has(field.id)).length;
                  const allSelected = groupSelected === group.fields.length;
                  return (
                    <section key={group.id} className="rounded-[18px] border border-black/10 bg-black/[0.018] p-3 dark:border-white/8 dark:bg-white/[0.026]">
                      <button
                        type="button"
                        onClick={() => toggleGroup(group.fields)}
                        className="flex w-full items-center justify-between gap-3 text-left"
                      >
                        <span className="text-[12px] font-black uppercase tracking-[0.14em] text-black/58 dark:text-white/52">{group.label}</span>
                        <span className={cn(
                          'flex h-5 min-w-5 items-center justify-center rounded-full border text-[10px] font-black',
                          allSelected
                            ? 'border-[var(--fm-accent)] bg-[var(--fm-accent)] text-white'
                            : 'border-black/14 text-black/36 dark:border-white/14 dark:text-white/32',
                        )}>
                          {allSelected ? <Check size={13} strokeWidth={3} /> : groupSelected}
                        </span>
                      </button>
                      <div className="mt-2.5 grid grid-cols-1 gap-2 sm:grid-cols-2">
                        {group.fields.map((field) => {
                          const active = selected.has(field.id);
                          return (
                            <button
                              key={field.id}
                              type="button"
                              onClick={() => toggleField(field.id)}
                              className={cn(
                                'flex min-h-10 items-center justify-between gap-3 rounded-[14px] border px-3 py-2 text-left transition',
                                active
                                  ? 'border-[var(--fm-accent)]/26 bg-[var(--fm-accent)]/10 text-black dark:text-white'
                                  : 'border-black/8 bg-white/50 text-black/54 hover:bg-white/80 dark:border-white/7 dark:bg-white/[0.035] dark:text-white/44 dark:hover:bg-white/[0.06]',
                              )}
                            >
                              <span className="text-[12px] font-bold leading-snug">{field.label}</span>
                              <span className={cn(
                                'flex h-4 w-4 shrink-0 items-center justify-center rounded-full border',
                                active ? 'border-[var(--fm-accent)] bg-[var(--fm-accent)] text-white' : 'border-black/14 dark:border-white/14',
                              )}>
                                {active && <Check size={10} strokeWidth={3} />}
                              </span>
                            </button>
                          );
                        })}
                      </div>
                    </section>
                  );
                })}
              </div>

              {(error || hasInvalidRange || !hasFields) && (
                <div className="mt-4 rounded-[14px] border border-[var(--fm-accent)]/24 bg-[var(--fm-accent)]/10 px-3 py-2.5 text-[12px] font-bold text-[var(--fm-accent-deep)] dark:text-[var(--fm-accent-soft)]">
                  {error || (hasInvalidRange ? 'Export start date must be before end date' : 'Select at least one field')}
                </div>
              )}
            </div>

            <div className="shrink-0 border-t border-black/10 bg-white/86 px-5 py-4 dark:border-white/8 dark:bg-[var(--fm-ink)]/90 md:px-6">
              <button
                type="button"
                onClick={onExport}
                disabled={isExporting || hasInvalidRange || !hasFields}
                className="flex w-full items-center justify-center gap-2 rounded-[18px] bg-[var(--fm-accent)] px-4 py-3 text-[12px] font-black uppercase tracking-[0.14em] text-white shadow-[0_12px_30px_-14px_rgb(var(--fm-accent-rgb)/0.7)] transition hover:bg-[var(--fm-accent-deep)] disabled:cursor-not-allowed disabled:opacity-50"
              >
                <Download size={16} strokeWidth={2.7} />
                {isExporting ? 'Preparing Workbook' : 'Download XLSX'}
              </button>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );

  if (!mounted) return null;
  return createPortal(dialog, document.body);
}
