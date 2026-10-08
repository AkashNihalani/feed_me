'use client';

/* ─────────────────────────────────────────────────────────────
   FEED MANAGE — every change you can make to your feeds, in one
   hook: start a feed (the multi-step Brief), rewrite a feed's
   Brief, add or remove a feeder, make one the anchor, delete a
   feed, export a feed to Excel, open the plan.

   Ported from the old Feed tab (FeedTabLegacy): the same
   endpoints, payloads and error messages. What changed is where
   the lists live: after a change lands, the feeds reload from
   lib/feedsStore instead of this hook keeping its own copy, and
   when the change moves posts in or out (a feeder added or
   removed, a feed deleted) the stream drops what it cached for
   that feed (lib/feedStream/store).

   A scope that points at something that is gone moves with it:
   delete the feed you are on and the tab goes back to all feeds;
   remove the feeder you are on and it goes back to their feed.
   ───────────────────────────────────────────────────────────── */

import { useMemo, useRef, useState } from 'react';
import { reloadFeeds, useFeeds, type AppFeed, type SlotUsage } from '@/lib/feedsStore';
import { invalidateStream } from '@/lib/feedStream/store';
import { istDay, shiftDay } from '@/lib/feedStream/contract';
import { getTabScope, setTabScope } from '@/lib/tabScope';
import { normalizeHandle } from '@/lib/feedLabels';
import { DEFAULT_EXPORT_FIELD_IDS, type ExportFieldId } from '@/lib/feedExportConfig';
import {
  CREATE_FEED_LAST_STEP,
  buildFeedBriefText,
  defaultFeedBrief,
  normalizeFeedBrief,
  type FeedBrief,
} from '@/components/feed/feedBriefUtils';

/* ── types ─────────────────────────────────────────────────── */

export type ManageAction = 'create' | 'edit' | 'add' | 'remove' | 'anchor' | 'delete';

// the change in flight (one at a time, as on the old tab)
export type ManagePending = { action: ManageAction; feedId: string | null; handle: string | null };

export type SlotSummary = { used: number; limit: number | null };

// what the create dialog's primary button did
export type CreateOutcome =
  | { kind: 'blocked' } // no name yet
  | { kind: 'next' } // moved on a step
  | { kind: 'created'; feedId: string | null } // the new feed's id, when it could be found in the reply
  | { kind: 'failed' };

type FeedBundle = { feeds?: Array<{ id?: unknown; title?: unknown }>; error?: string };

// the Brief fields the feeds endpoint sends (typed loosely: the store may type them narrower or not at all)
type BriefFields = { feedBrief?: unknown; contextBrief?: unknown; feedBriefText?: unknown; contextBible?: unknown };

type MutationPlan = {
  action: ManageAction;
  payload: Record<string, unknown>;
  feedId: string | null;
  handle?: string | null;
  fallbackError: string;
  timeoutMs?: number;
  // once the server has the change, before the lists reload (a scope moving off what is gone)
  settle?: () => void;
  // the feed whose stream to drop (null: every scope), only when the change moves posts in or out of a scope.
  // A dropped scope blanks and reloads (and the reader loses their place), so a change the stream can't see —
  // a Brief, the anchor, a new empty feed — leaves it alone and only reloads the feeds
  invalidate?: string | null;
};

/* ── helpers (from the old tab) ────────────────────────────── */

// the last 90 IST days, today included
function defaultExportRange() {
  const to = istDay();
  return { from: shiftDay(to, -89), to };
}

function downloadFilenameFromDisposition(value: string | null): string {
  if (!value) return 'feed_export.xlsx';
  const utfMatch = /filename\*=UTF-8''([^;]+)/i.exec(value);
  if (utfMatch?.[1]) return decodeURIComponent(utfMatch[1].trim().replace(/^"|"$/g, ''));
  const quotedMatch = /filename="([^"]+)"/i.exec(value);
  if (quotedMatch?.[1]) return quotedMatch[1].trim();
  const plainMatch = /filename=([^;]+)/i.exec(value);
  if (plainMatch?.[1]) return plainMatch[1].trim().replace(/^"|"$/g, '');
  return 'feed_export.xlsx';
}

// "@Handle", "handle", or a pasted profile link → "handle"
export function cleanHandle(value: string | null | undefined): string {
  const raw = String(value || '').trim();
  const fromLink = /instagram\.com\/([^/?#\s]+)/i.exec(raw)?.[1];
  return normalizeHandle(fromLink ?? raw);
}

function briefFields(feed: AppFeed): BriefFields {
  return feed as AppFeed & BriefFields;
}

// the feed's saved Brief, as the dialog edits it
export function feedBriefOf(feed: AppFeed): FeedBrief {
  const fields = briefFields(feed);
  return normalizeFeedBrief(fields.feedBrief || fields.contextBrief);
}

// the Brief's written summary, '' when none was saved
export function feedBriefTextOf(feed: AppFeed): string {
  const fields = briefFields(feed);
  return String(fields.feedBriefText || fields.contextBible || '').trim();
}

function feedBibleOf(feed: AppFeed, brief: FeedBrief): string {
  return feedBriefTextOf(feed) || buildFeedBriefText(brief, feed.title || 'this feed');
}

function errorMessage(error: unknown, fallback: string): string {
  if (error instanceof DOMException && error.name === 'AbortError') return 'Timed out';
  return error instanceof Error && error.message ? error.message : fallback;
}

// slots in use: the plan's count when the server sends one, else one per feeder (how the plan bills)
export function summarizeSlots(feeds: AppFeed[] | null, slots: SlotUsage | null): SlotSummary {
  const feeders = (feeds ?? []).reduce((total, feed) => total + feed.feeders.length, 0);
  const used = slots && slots.used > 0 ? slots.used : feeders;
  const limit = slots?.limit != null && slots.limit > 0 ? slots.limit : null;
  return { used, limit };
}

// the feed a create added: the id the reply has that the list before it didn't; by its name when the list before
// it wasn't loaded yet
function createdFeedId(bundle: FeedBundle, before: Set<string> | null, title: string): string | null {
  const ids = (bundle.feeds ?? []).map((feed) => ({ id: String(feed.id ?? ''), title: String(feed.title ?? '') }));
  const fresh = before ? ids.find((feed) => feed.id && !before.has(feed.id)) : undefined;
  if (fresh) return fresh.id;
  const named = ids.find((feed) => feed.id && feed.title.toUpperCase() === title.toUpperCase());
  return named?.id ?? null;
}

// the lists catch up with a change: the feeds reload, the stream drops what it cached for the feed (when asked)
async function syncAfterChange(invalidate: string | null | undefined) {
  try {
    const reloading = reloadFeeds();
    if (invalidate !== undefined) invalidateStream(invalidate);
    await reloading;
  } catch {
    // the change landed; the lists catch up on their next load
  }
}

/* ── the hook ──────────────────────────────────────────────── */

export function useFeedManage() {
  const { feeds, slots } = useFeeds();

  /* busy / error — one change at a time, the way the old tab ran them */
  const busyRef = useRef(false);
  const [busy, setBusy] = useState(false);
  const [pending, setPending] = useState<ManagePending | null>(null);
  const [failure, setFailure] = useState<{ action: ManageAction; message: string } | null>(null);

  /* create a feed */
  const [isCreatingFeed, setIsCreatingFeed] = useState(false);
  const [newFeedName, setNewFeedName] = useState('');
  const [newFeedBrief, setNewFeedBrief] = useState<FeedBrief>(() => defaultFeedBrief());
  const [createFeedStep, setCreateFeedStep] = useState(0);
  const [newFeedBibleDraft, setNewFeedBibleDraft] = useState('');

  /* rewrite a feed's Brief */
  const [editFeedId, setEditFeedId] = useState<string | null>(null);
  const [editFeedBrief, setEditFeedBrief] = useState<FeedBrief>(() => defaultFeedBrief());
  const [editFeedStep, setEditFeedStep] = useState(0);
  const [editFeedBibleDraft, setEditFeedBibleDraft] = useState('');

  /* export */
  const [exportTarget, setExportTarget] = useState<{ feedId: string; handle: string | null } | null>(null);
  const [exportFrom, setExportFrom] = useState<string>(() => defaultExportRange().from);
  const [exportTo, setExportTo] = useState<string>(() => defaultExportRange().to);
  const [exportFields, setExportFields] = useState<ExportFieldId[]>(() => [...DEFAULT_EXPORT_FIELD_IDS]);
  const [includeExportSummary, setIncludeExportSummary] = useState(true);
  const [isExporting, setIsExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);

  /* the plan (Fund) */
  const [isFundPanelOpen, setIsFundPanelOpen] = useState(false);

  const editFeed = useMemo(
    () => (editFeedId ? feeds?.find((feed) => feed.id === editFeedId) ?? null : null),
    [editFeedId, feeds],
  );
  const exportFeed = useMemo(
    () => (exportTarget ? feeds?.find((feed) => feed.id === exportTarget.feedId) ?? null : null),
    [exportTarget, feeds],
  );
  const newFeedBriefPreview = useMemo(
    () => buildFeedBriefText(newFeedBrief, newFeedName.trim() || 'this feed'),
    [newFeedBrief, newFeedName],
  );
  const editFeedBriefPreview = useMemo(
    () => buildFeedBriefText(editFeedBrief, editFeed?.title || 'this feed'),
    [editFeed?.title, editFeedBrief],
  );
  const slotSummary = useMemo(() => summarizeSlots(feeds, slots), [feeds, slots]);

  /* the one way a change goes out (runFeedAction + handleAddFeeder on the old tab) */
  const mutate = async (plan: MutationPlan): Promise<FeedBundle | null> => {
    if (busyRef.current) return null;
    busyRef.current = true;
    setBusy(true);
    setPending({ action: plan.action, feedId: plan.feedId, handle: plan.handle ?? null });
    setFailure(null);
    const controller = new AbortController();
    const timer = plan.timeoutMs ? window.setTimeout(() => controller.abort(), plan.timeoutMs) : null;
    try {
      const res = await fetch('/api/feed', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(plan.payload),
        signal: controller.signal,
      });
      const json = (await res.json().catch(() => ({}))) as FeedBundle;
      if (!res.ok) throw new Error(json.error || plan.fallbackError);
      plan.settle?.();
      await syncAfterChange(plan.invalidate);
      return json;
    } catch (error) {
      setFailure({ action: plan.action, message: errorMessage(error, plan.fallbackError) });
      return null;
    } finally {
      if (timer != null) window.clearTimeout(timer);
      busyRef.current = false;
      setBusy(false);
      setPending(null);
    }
  };

  /* ── feeders ── */

  const addFeeder = async (feedId: string, handle: string): Promise<boolean> => {
    const clean = cleanHandle(handle);
    if (!clean) return false;
    const bundle = await mutate({
      action: 'add',
      payload: { action: 'add_feeder', feedId: Number(feedId), handle: clean },
      feedId,
      handle: clean,
      fallbackError: 'Unable to add feeder',
      timeoutMs: 30000,
      invalidate: feedId,
    });
    return bundle != null;
  };

  const removeFeeder = async (feedId: string, handle: string): Promise<boolean> => {
    const bundle = await mutate({
      action: 'remove',
      payload: { action: 'remove_feeder', feedId: Number(feedId), handle },
      feedId,
      handle,
      fallbackError: 'Action failed',
      settle: () => {
        const scope = getTabScope();
        if (scope.feedId === feedId && scope.handle != null && normalizeHandle(scope.handle) === normalizeHandle(handle)) {
          setTabScope({ feedId, handle: null });
        }
      },
      invalidate: feedId,
    });
    return bundle != null;
  };

  const makeAnchor = async (feedId: string, handle: string): Promise<boolean> => {
    const bundle = await mutate({
      action: 'anchor',
      payload: { action: 'make_anchor', feedId: Number(feedId), handle },
      feedId,
      handle,
      fallbackError: 'Action failed',
    });
    return bundle != null;
  };

  /* ── delete a feed (confirmDeleteFeed; the confirm step itself lives in the sheet) ── */

  const deleteFeed = async (feedId: string): Promise<boolean> => {
    const bundle = await mutate({
      action: 'delete',
      payload: { action: 'delete_feed', feedId: Number(feedId) },
      feedId,
      fallbackError: 'Action failed',
      settle: () => {
        if (getTabScope().feedId === feedId) setTabScope({ feedId: null, handle: null });
      },
      // its posts leave its own scopes and all feeds (the store drops both for a feed id)
      invalidate: feedId,
    });
    return bundle != null;
  };

  /* ── create a feed (handleCreateFeed and the create modal) ── */

  const resetCreateFeedModal = () => {
    setIsCreatingFeed(false);
    setNewFeedName('');
    setNewFeedBrief(defaultFeedBrief());
    setCreateFeedStep(0);
    setNewFeedBibleDraft('');
  };

  // false when the person kept their draft
  const closeCreateFeedModal = (): boolean => {
    const dirty = Boolean(
      newFeedName.trim()
      || JSON.stringify(newFeedBrief) !== JSON.stringify(defaultFeedBrief())
      || newFeedBibleDraft.trim(),
    );
    if (dirty && typeof window !== 'undefined' && !window.confirm('Discard this Brief?')) return false;
    resetCreateFeedModal();
    return true;
  };

  const handleCreateFeed = async (): Promise<CreateOutcome> => {
    if (!newFeedName.trim()) return { kind: 'blocked' };
    if (createFeedStep < CREATE_FEED_LAST_STEP) {
      const nextStep = createFeedStep + 1;
      if (nextStep === CREATE_FEED_LAST_STEP) {
        setNewFeedBibleDraft('');
      }
      setCreateFeedStep(nextStep);
      return { kind: 'next' };
    }
    const title = newFeedName.trim();
    const before = feeds ? new Set(feeds.map((feed) => feed.id)) : null;
    const bundle = await mutate({
      action: 'create',
      payload: {
        action: 'create_feed',
        title,
        feedBrief: newFeedBrief,
        feedBriefText: newFeedBibleDraft.trim() || newFeedBriefPreview,
      },
      feedId: null,
      fallbackError: 'Action failed',
    });
    if (!bundle) return { kind: 'failed' };
    resetCreateFeedModal();
    return { kind: 'created', feedId: createdFeedId(bundle, before, title) };
  };

  /* ── rewrite a Brief (openFeedContextEditor / handleSaveFeedContext) ── */

  const openFeedContextEditor = (feedId: string) => {
    const feed = feeds?.find((item) => item.id === feedId);
    if (!feed) return;
    const brief = feedBriefOf(feed);
    setEditFeedBrief(brief);
    setEditFeedStep(0);
    setEditFeedBibleDraft(feedBibleOf(feed, brief));
    setEditFeedId(feed.id);
  };

  // false when the person kept their changes
  const closeFeedContextEditor = (): boolean => {
    if (!editFeed) {
      setEditFeedId(null);
      return true;
    }
    const originalBrief = feedBriefOf(editFeed);
    const originalBible = feedBibleOf(editFeed, originalBrief);
    const dirty = JSON.stringify(editFeedBrief) !== JSON.stringify(originalBrief)
      || editFeedBibleDraft.trim() !== originalBible.trim();
    if (dirty && typeof window !== 'undefined' && !window.confirm('Discard changes to this Brief?')) return false;
    setEditFeedId(null);
    return true;
  };

  const handleSaveFeedContext = async (): Promise<boolean> => {
    if (!editFeed) return false;
    if (editFeedStep < CREATE_FEED_LAST_STEP) {
      const nextStep = editFeedStep + 1;
      if (nextStep === CREATE_FEED_LAST_STEP && !editFeedBibleDraft.trim()) {
        setEditFeedBibleDraft('');
      }
      setEditFeedStep(nextStep);
      return false;
    }
    const bundle = await mutate({
      action: 'edit',
      payload: {
        action: 'update_feed_context',
        feedId: Number(editFeed.id),
        title: editFeed.title,
        feedBrief: editFeedBrief,
        feedBriefText: editFeedBibleDraft.trim() || editFeedBriefPreview,
      },
      feedId: editFeed.id,
      fallbackError: 'Action failed',
    });
    if (bundle) setEditFeedId(null);
    return bundle != null;
  };

  const startOverFeedContext = () => {
    if (!editFeed) return;
    const brief = feedBriefOf(editFeed);
    setEditFeedBrief(brief);
    setEditFeedStep(0);
    setEditFeedBibleDraft(feedBibleOf(editFeed, brief));
  };

  /* ── export (handleDownloadExport) ── */

  const openExport = (feedId: string, handle: string | null = null) => {
    setExportError(null);
    setExportTarget({ feedId, handle: handle ? normalizeHandle(handle) : null });
  };

  const closeExport = () => {
    if (isExporting) return;
    setExportTarget(null);
    setExportError(null);
  };

  const handleDownloadExport = async () => {
    if (!exportTarget) return;
    if (exportFrom && exportTo && exportFrom > exportTo) {
      setExportError('Export start date must be before end date');
      return;
    }
    if (exportFields.length === 0) {
      setExportError('Select at least one field');
      return;
    }
    setExportError(null);
    setIsExporting(true);
    const params = new URLSearchParams();
    if (exportTarget.handle) params.set('handle', exportTarget.handle);
    if (exportFrom) params.set('from', exportFrom);
    if (exportTo) params.set('to', exportTo);
    params.set('fields', exportFields.join(','));
    params.set('sheets', includeExportSummary ? 'summary,posts' : 'posts');
    const query = params.toString();
    try {
      const res = await fetch(`/api/download/${exportTarget.feedId}${query ? `?${query}` : ''}`);
      if (!res.ok) {
        let message = 'Failed to generate export';
        try {
          const json = await res.json();
          if (json?.error) message = String(json.error);
        } catch {
          message = res.statusText || message;
        }
        throw new Error(message);
      }
      const blob = await res.blob();
      const filename = downloadFilenameFromDisposition(res.headers.get('Content-Disposition'));
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 0);
      setExportTarget(null);
    } catch (err) {
      setExportError(err instanceof Error ? err.message : 'Failed to generate export');
    } finally {
      setIsExporting(false);
    }
  };

  const exportScopeLabel = exportTarget
    ? `${exportFeed?.title ? `${exportFeed.title} · ` : ''}${exportTarget.handle ? `@${exportTarget.handle.toUpperCase()}` : 'FULL FEED'}`
    : 'FULL FEED';

  /* ── everything closes with the sheet (no discard prompts: the sheet is already going) ── */

  const dismissAll = () => {
    resetCreateFeedModal();
    setEditFeedId(null);
    if (!isExporting) {
      setExportTarget(null);
      setExportError(null);
    }
    setIsFundPanelOpen(false);
  };

  return {
    feeds,
    slots: slotSummary,

    busy,
    pending,
    error: failure?.message ?? null,
    errorAction: failure?.action ?? null,
    clearError: () => setFailure(null),

    create: {
      open: isCreatingFeed,
      name: newFeedName,
      brief: newFeedBrief,
      step: createFeedStep,
      bibleDraft: newFeedBibleDraft,
      start: () => setIsCreatingFeed(true),
      close: closeCreateFeedModal,
      // stable setters: the dialog keeps some of them in effect dependencies
      setName: setNewFeedName,
      setStep: setCreateFeedStep,
      setBibleDraft: setNewFeedBibleDraft,
      updateBrief: (patch: Partial<FeedBrief>) => setNewFeedBrief((current) => ({ ...current, ...patch })),
      back: () => setCreateFeedStep((step) => Math.max(0, step - 1)),
      startOver: () => {
        setNewFeedName('');
        setNewFeedBrief(defaultFeedBrief());
        setCreateFeedStep(0);
        setNewFeedBibleDraft('');
      },
      primary: handleCreateFeed,
    },

    editBrief: {
      open: editFeedId != null && editFeed != null,
      feed: editFeed,
      brief: editFeedBrief,
      step: editFeedStep,
      bibleDraft: editFeedBibleDraft,
      start: openFeedContextEditor,
      close: closeFeedContextEditor,
      setStep: setEditFeedStep,
      setBibleDraft: setEditFeedBibleDraft,
      updateBrief: (patch: Partial<FeedBrief>) => setEditFeedBrief((current) => ({ ...current, ...patch })),
      back: () => setEditFeedStep((current) => Math.max(0, current - 1)),
      startOver: startOverFeedContext,
      primary: handleSaveFeedContext,
    },

    addFeeder,
    removeFeeder,
    makeAnchor,
    deleteFeed,

    exporter: {
      open: exportTarget != null,
      feedId: exportTarget?.feedId ?? null,
      handle: exportTarget?.handle ?? null,
      scopeLabel: exportScopeLabel,
      from: exportFrom,
      to: exportTo,
      fields: exportFields,
      includeSummary: includeExportSummary,
      isExporting,
      error: exportError,
      start: openExport,
      close: closeExport,
      setFrom: setExportFrom,
      setTo: setExportTo,
      setFields: setExportFields,
      setIncludeSummary: setIncludeExportSummary,
      download: handleDownloadExport,
    },

    fund: {
      open: isFundPanelOpen,
      show: () => setIsFundPanelOpen(true),
      hide: () => setIsFundPanelOpen(false),
    },

    dismissAll,
  };
}

export type FeedManage = ReturnType<typeof useFeedManage>;
