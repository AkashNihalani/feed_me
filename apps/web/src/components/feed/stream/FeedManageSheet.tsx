'use client';

/* ─────────────────────────────────────────────────────────────
   FEED MANAGE SHEET — every change to your feeds, behind the
   scope bar's Manage button. What it offers follows the scope:
     all feeds → start a feed, the plan and its slots, the app's
                 appearance (light · dark · auto), your feeds
     a feed     → add a feeder, its Brief, export, delete it
     a feeder   → make them the anchor, their page, remove them
   Opened with the create intent (the empty state's button) it
   goes straight into the Brief, and only rises once the feed is
   made, on the new feed, ready for its first feeders.

   A bottom sheet on a phone, a centred panel from 1024px. The
   panel slides (transform) over a dim (opacity); a confirm step
   cross-fades over the rows in place, so the sheet never changes
   height. Nothing blurs, nothing loops. The logic is the old Feed
   tab's, ported into useFeedManage.
   ───────────────────────────────────────────────────────────── */

import './stream.css';
import Link from 'next/link';
import dynamic from 'next/dynamic';
import {
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
  useSyncExternalStore,
  type CSSProperties,
  type ReactNode,
  type RefObject,
} from 'react';
import { createPortal } from 'react-dom';
import { useReducedMotion } from 'framer-motion';
import {
  ArrowUp,
  ArrowUpRight,
  ChevronRight,
  Contrast,
  CreditCard,
  Crown,
  Download,
  FileText,
  Moon,
  Plus,
  Sun,
  SunMoon,
  Trash2,
  UserMinus,
  X,
} from 'lucide-react';
import FeedBriefDialog from '@/components/feed/FeedBriefDialog';
import FeedExportDialog from '@/components/feed/FeedExportDialog';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import type { AppFeed, AppFeeder } from '@/lib/feedsStore';
import { formatCount, type StreamScope } from '@/lib/feedStream/contract';
import { feedInitials, normalizeHandle, titleCase } from '@/lib/feedLabels';
import { useAppHaptics } from '@/lib/haptics';
import { BEAT_EASE_CSS, BEAT_LEAVE_EASE } from '@/lib/motion';
import { setTabScope } from '@/lib/tabScope';
import { setThemePreference, useResolvedTheme, useThemePreference, type ThemePreference } from '@/lib/theme';
import { cn } from '@/lib/utils';
import { cleanHandle, feedBriefTextOf, useFeedManage, type SlotSummary } from './useFeedManage';

// the plan is a big page: its code loads the first time the sheet could need it, not with the Feed tab
const FundPanelOverlay = dynamic(() => import('@/components/feed/FundPanelOverlay'), { ssr: false });
const loadFundPanel = () => import('@/components/feed/FundPanelOverlay');

export type FeedManageSheetProps = {
  open: boolean;
  scope: StreamScope;
  intent?: 'create-feed' | null;
  onClose: () => void;
};

/* ── motion ────────────────────────────────────────────────── */

const OPEN_MS = 380;
const CLOSE_MS = 280;
const SWAP_MS = 320;
const EASE_OPEN = BEAT_EASE_CSS; // cubic-bezier(0.16, 0.9, 0.2, 1)
const EASE_CLOSE = `cubic-bezier(${BEAT_LEAVE_EASE.join(', ')})`;

/* ── surface ───────────────────────────────────────────────── */

// the stream's tokens (stream.css, on .fm-stream: surface-2 is the raised sheet), with dark fallbacks; the layout is
// inline so nothing a stylesheet puts on .fm-stream can move the overlay
const ROOT_STYLE = {
  position: 'fixed',
  inset: 0,
  zIndex: 200,
  display: 'block',
  overflow: 'hidden',
  isolation: 'isolate',
  background: 'transparent',
  color: 'var(--ms-text)',
  '--ms-sheet': 'var(--st-surface-2, #16161a)',
  '--ms-chip': 'rgb(var(--fm-fg-rgb, 255 255 255) / 0.06)',
  '--ms-line': 'var(--st-line, rgba(255, 255, 255, 0.09))',
  '--ms-text': 'var(--st-text, #ffffff)',
  '--ms-text-2': 'var(--st-text-2, rgba(255, 255, 255, 0.65))',
  '--ms-text-3': 'var(--st-text-3, rgba(255, 255, 255, 0.45))',
} as CSSProperties;

const FEED_BADGE_STYLE: CSSProperties = {
  color: 'var(--fm-accent)',
  background: 'radial-gradient(circle at 30% 18%, rgb(var(--fm-accent-rgb) / 0.12), transparent 58%), linear-gradient(135deg,#f7f7f8,#cfd3dc)',
};

const FOCUSABLE = 'a[href], button:not([disabled]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])';

/* ── helpers ───────────────────────────────────────────────── */

const subscribeNothing = () => () => {};
const onClient = () => true;
const onServer = () => false;

type View = 'all' | 'feed' | 'feeder';
type Resolved = { view: View; feed: AppFeed | null; feeder: AppFeeder | null };

// the scope as this sheet reads it; a feed or feeder that isn't in the list (any more) reads as all feeds
function resolveScope(scope: StreamScope, feeds: AppFeed[] | null): Resolved {
  if (!feeds) return { view: 'all', feed: null, feeder: null };
  const handle = scope.handle ? normalizeHandle(scope.handle) : null;
  const feed = scope.feedId
    ? feeds.find((item) => item.id === scope.feedId) ?? null
    : handle
      ? feeds.find((item) => item.feeders.some((feeder) => normalizeHandle(feeder.handle) === handle)) ?? null
      : null;
  if (!feed) return { view: 'all', feed: null, feeder: null };
  const feeder = handle ? feed.feeders.find((item) => normalizeHandle(item.handle) === handle) ?? null : null;
  return feeder ? { view: 'feeder', feed, feeder } : { view: 'feed', feed, feeder: null };
}

function plural(count: number, noun: string) {
  return `${count} ${noun}${count === 1 ? '' : 's'}`;
}

function slotsInUse(slots: SlotSummary) {
  return slots.limit != null ? `${slots.used} of ${slots.limit} in use` : `${slots.used} in use`;
}

function slotsLine(slots: SlotSummary) {
  return slots.limit != null ? `${slots.used} of ${slots.limit} slots in use` : `${plural(slots.used, 'slot')} in use`;
}

function anchorOf(feed: AppFeed) {
  return feed.feeders.find((feeder) => feeder.isAnchor) ?? null;
}

function feedSupport(feed: AppFeed) {
  if (feed.feeders.length === 0) return 'No feeders yet';
  const anchor = anchorOf(feed);
  return `${plural(feed.feeders.length, 'feeder')}${anchor ? ` · anchor @${anchor.handle}` : ''}`;
}

// keyboard focus stays inside the open sheet
function trapTab(event: KeyboardEvent, panel: HTMLElement | null) {
  if (!panel) return;
  const items = Array.from(panel.querySelectorAll<HTMLElement>(FOCUSABLE))
    .filter((element) => !element.closest('[inert]') && element.getClientRects().length > 0);
  if (items.length === 0) {
    event.preventDefault();
    panel.focus({ preventScroll: true });
    return;
  }
  const first = items[0];
  const last = items[items.length - 1];
  const active = document.activeElement;
  const inside = active instanceof Node && panel.contains(active) && active !== panel;
  if (event.shiftKey) {
    if (!inside || active === first) {
      event.preventDefault();
      last.focus({ preventScroll: true });
    }
  } else if (!inside || active === last) {
    event.preventDefault();
    first.focus({ preventScroll: true });
  }
}

/* ── pieces ────────────────────────────────────────────────── */

type Tone = 'plain' | 'primary' | 'danger' | 'anchor';

function RowIcon({ tone, children }: { tone: Tone; children: ReactNode }) {
  return (
    <span
      aria-hidden="true"
      className={cn(
        'relative grid h-9 w-9 shrink-0 place-items-center rounded-[10px] border transition-transform duration-150 ease-out group-active:scale-[0.94]',
        tone === 'primary' && 'border-transparent bg-(--fm-accent) text-white',
        (tone === 'danger' || tone === 'anchor') && 'border-[rgb(var(--fm-accent-rgb)/0.28)] bg-[rgb(var(--fm-accent-rgb)/0.12)] text-(--fm-accent-text)',
        tone === 'plain' && 'border-(--ms-line) bg-(--ms-chip) text-(--ms-text-2)',
      )}
    >
      {children}
    </span>
  );
}

const ROW_CLASS = 'group relative flex min-h-[56px] w-full items-center gap-3 rounded-[14px] px-3 py-2 text-left outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-(--fm-accent-bright) disabled:cursor-default';

type RowProps = {
  icon?: ReactNode;
  media?: ReactNode; // in place of the icon tile (a feed's badge)
  tone?: Tone;
  label: ReactNode;
  support?: ReactNode;
  alert?: boolean; // the support line is an error
  trailing?: ReactNode;
  disabled?: boolean;
  dimWhenDisabled?: boolean;
  href?: string;
  onClick?: () => void;
};

function Row({ icon, media, tone = 'plain', label, support, alert = false, trailing, disabled = false, dimWhenDisabled = true, href, onClick }: RowProps) {
  const body = (
    <>
      {/* the press: an overlay that fades, so nothing repaints its background */}
      <span
        aria-hidden="true"
        className="pointer-events-none absolute inset-0 rounded-[14px] bg-fg/[0.06] opacity-0 transition-opacity duration-150 group-hover:opacity-70 group-active:opacity-100 group-disabled:opacity-0"
      />
      {media ?? <RowIcon tone={tone}>{icon}</RowIcon>}
      <span className="relative min-w-0 flex-1">
        <span className={cn('block truncate text-[16px] font-semibold leading-5', tone === 'danger' ? 'text-(--fm-accent-text)' : 'text-(--ms-text)')}>
          {label}
        </span>
        {support ? (
          <span className={cn('mt-0.5 block truncate text-[12px] font-medium leading-4', alert ? 'text-(--fm-accent-text)' : 'text-(--ms-text-2)')}>
            {support}
          </span>
        ) : null}
      </span>
      {trailing ? <span className="relative flex shrink-0 items-center gap-2">{trailing}</span> : null}
    </>
  );
  if (href) {
    return (
      <Link href={href} onClick={onClick} className={ROW_CLASS}>
        {body}
      </Link>
    );
  }
  return (
    <button type="button" onClick={onClick} disabled={disabled} className={cn(ROW_CLASS, dimWhenDisabled && 'disabled:opacity-45')}>
      {body}
    </button>
  );
}

function Chevron() {
  return <ChevronRight aria-hidden="true" size={16} strokeWidth={2.4} className="text-(--ms-text-3)" />;
}

function Divider() {
  return <div aria-hidden="true" className="mx-3 my-2 h-px bg-(--ms-line)" />;
}

function FeedBadge({ title }: { title: string }) {
  return (
    <span
      aria-hidden="true"
      className="relative grid h-9 w-9 shrink-0 place-items-center rounded-full text-[10px] font-black leading-none transition-transform duration-150 ease-out group-active:scale-[0.94]"
      style={FEED_BADGE_STYLE}
    >
      {feedInitials(title)}
    </span>
  );
}

// the feed's first faces, anchor first
function FaceStack({ feeders }: { feeders: AppFeeder[] }) {
  const faces = [...feeders].sort((a, b) => Number(b.isAnchor) - Number(a.isAnchor)).slice(0, 3);
  if (faces.length === 0) return null;
  return (
    <span aria-hidden="true" className="flex -space-x-2">
      {faces.map((feeder) => (
        <span key={feeder.handle} className="relative block h-6 w-6 overflow-hidden rounded-full border-2 border-(--ms-sheet)">
          <FeederStoryAvatar feeder={feeder} className="text-[8px]" />
        </span>
      ))}
    </span>
  );
}

type AddFeederRowProps = {
  value: string;
  adding: string | null; // the handle on its way in
  error: string | null;
  added: string | null; // the last handle that made it in
  blocked: boolean; // another change is running
  slots: SlotSummary;
  onChange: (value: string) => void;
  onSubmit: () => void;
};

// the inline @handle field: reads as a row until you tap it, submits on Enter (or the arrow)
function AddFeederRow({ value, adding, error, added, blocked, slots, onChange, onSubmit }: AddFeederRowProps) {
  const supportId = useId();
  const [focused, setFocused] = useState(false);
  const clean = cleanHandle(value);
  const support = adding
    ? `Adding @${adding}, checking the profile…`
    : error
      ? error
      : clean
        ? `Adds @${clean} · uses 1 slot`
        : added
          ? `@${added} added · ${slotsInUse(slots)}`
          : `Uses 1 slot · ${slotsInUse(slots)}`;
  const canSubmit = Boolean(clean) && !adding && !blocked;

  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        if (canSubmit) onSubmit();
      }}
      className="group/add relative flex min-h-[56px] items-center gap-3 rounded-[14px] px-3 py-2"
    >
      <span aria-hidden="true" className="pointer-events-none absolute inset-0 rounded-[14px] bg-fg/[0.045] opacity-0 transition-opacity duration-150 group-focus-within/add:opacity-100" />
      <RowIcon tone="primary">
        <span className="text-[16px] font-black leading-none">@</span>
      </RowIcon>
      <label className="relative min-w-0 flex-1 cursor-text">
        <input
          type="text"
          name="handle"
          value={value}
          readOnly={Boolean(adding)}
          onChange={(event) => onChange(event.target.value)}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          placeholder={focused ? '@handle' : 'Add feeder'}
          aria-label="Add a feeder by their Instagram handle"
          aria-describedby={supportId}
          autoComplete="off"
          autoCapitalize="none"
          autoCorrect="off"
          spellCheck={false}
          enterKeyHint="go"
          className={cn(
            'block h-5 w-full min-w-0 select-text bg-transparent p-0 text-[16px] font-semibold leading-5 text-(--ms-text) outline-none',
            focused ? 'placeholder:text-(--ms-text-3)' : 'placeholder:text-(--ms-text)',
            adding && 'text-(--ms-text-2)',
          )}
        />
        <span
          id={supportId}
          aria-live="polite"
          className={cn('mt-0.5 block truncate text-[12px] font-medium leading-4', error && !adding ? 'text-(--fm-accent-text)' : 'text-(--ms-text-2)')}
        >
          {support}
        </span>
      </label>
      <button
        type="submit"
        disabled={!canSubmit}
        aria-label={clean ? `Add @${clean}` : 'Add feeder'}
        className="relative grid h-9 w-9 shrink-0 place-items-center rounded-[10px] border border-(--ms-line) bg-(--ms-chip) text-(--ms-text) outline-none transition-[opacity,scale] duration-150 ease-out active:scale-[0.94] disabled:opacity-30 focus-visible:ring-2 focus-visible:ring-(--fm-accent-bright)"
      >
        <ArrowUp size={16} strokeWidth={2.6} />
      </button>
    </form>
  );
}

/* the app's theme: three choices in a sunken track, the current one under a rose pill that glides to it (transform
   only). A pick redraws the app as a circle growing out of the button (lib/theme) */
const THEMES: ReadonlyArray<{ value: ThemePreference; label: string; Icon: typeof Sun }> = [
  { value: 'light', label: 'Light', Icon: Sun },
  { value: 'dark', label: 'Dark', Icon: Moon },
  { value: 'auto', label: 'Auto, match this device', Icon: SunMoon },
];

function AppearanceRow({ onPick }: { onPick: () => void }) {
  const preference = useThemePreference();
  const resolved = useResolvedTheme();
  const at = Math.max(0, THEMES.findIndex((theme) => theme.value === preference));
  const buttons = useRef<Array<HTMLButtonElement | null>>([]);
  const support = preference === 'auto' ? `Follows this device · ${resolved === 'dark' ? 'dark' : 'light'}` : preference === 'dark' ? 'Always dark' : 'Always light';

  const pick = (index: number) => {
    const next = THEMES[index];
    const button = buttons.current[index];
    if (!next || next.value === preference) return;
    onPick();
    const rect = button?.getBoundingClientRect();
    setThemePreference(next.value, rect ? { x: rect.left + rect.width / 2, y: rect.top + rect.height / 2 } : undefined);
  };

  return (
    <div className="relative flex min-h-[56px] w-full items-center gap-3 rounded-[14px] px-3 py-2">
      <RowIcon tone="plain">
        <Contrast size={17} strokeWidth={2.3} />
      </RowIcon>
      <span className="relative min-w-0 flex-1">
        <span className="block truncate text-[16px] font-semibold leading-5 text-(--ms-text)">Appearance</span>
        <span className="mt-0.5 block truncate text-[12px] font-medium leading-4 text-(--ms-text-2)">{support}</span>
      </span>
      <div
        role="radiogroup"
        aria-label="Appearance"
        className="relative grid h-9 w-[126px] shrink-0 grid-cols-3 rounded-[14px] border border-(--ms-line) bg-(--fm-well) p-[3px] shadow-(--fm-well-shade)"
        onKeyDown={(event) => {
          const step = event.key === 'ArrowRight' || event.key === 'ArrowDown' ? 1 : event.key === 'ArrowLeft' || event.key === 'ArrowUp' ? -1 : 0;
          if (!step) return;
          event.preventDefault();
          const next = (at + step + THEMES.length) % THEMES.length;
          buttons.current[next]?.focus();
          pick(next);
        }}
      >
        <span
          aria-hidden="true"
          className="pointer-events-none absolute bottom-[3px] left-[3px] top-[3px] w-[calc((100%-6px)/3)] rounded-[10px] bg-(--fm-accent) shadow-[inset_0_1px_0_rgb(255_255_255/0.28)] transition-transform duration-300 ease-[cubic-bezier(0.16,0.9,0.2,1)]"
          style={{ transform: `translateX(${at * 100}%)` }}
        />
        {THEMES.map(({ value, label, Icon }, index) => {
          const on = index === at;
          return (
            <button
              key={value}
              ref={(node) => {
                buttons.current[index] = node;
              }}
              type="button"
              role="radio"
              aria-checked={on}
              aria-label={label}
              tabIndex={on ? 0 : -1}
              onClick={() => pick(index)}
              className={cn(
                'relative z-10 grid h-full min-w-0 place-items-center rounded-[10px] outline-none [-webkit-tap-highlight-color:transparent] transition-[color,scale] duration-200 ease-out active:scale-[0.92] focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-(--fm-accent-bright)',
                on ? 'text-white' : 'text-(--ms-text-2)',
              )}
            >
              <Icon size={16} strokeWidth={2.4} aria-hidden="true" />
            </button>
          );
        })}
      </div>
    </div>
  );
}

type ConfirmViewProps = {
  title: string;
  body: string;
  action: string;
  working: string;
  busy: boolean; // this change is on its way
  blocked: boolean; // another change is running
  error: string | null;
  cancelRef: RefObject<HTMLButtonElement | null>;
  onCancel: () => void;
  onConfirm: () => void;
};

function ConfirmView({ title, body, action, working, busy, blocked, error, cancelRef, onCancel, onConfirm }: ConfirmViewProps) {
  return (
    <div className="px-3 pb-1 pt-2">
      <div className="text-[22px] font-black leading-[1.1] tracking-[-0.04em] text-(--ms-text)">{title}</div>
      <p className="mt-2 text-[14px] font-medium leading-5 text-(--ms-text-2)">{body}</p>
      {error ? <p role="alert" className="mt-3 text-[12px] font-semibold leading-4 text-(--fm-accent-text)">{error}</p> : null}
      <div className="mt-5 grid grid-cols-2 gap-2">
        <button
          ref={cancelRef}
          type="button"
          onClick={onCancel}
          disabled={busy}
          className="h-12 rounded-[14px] border border-(--ms-line) bg-(--ms-chip) text-[14px] font-semibold text-(--ms-text) outline-none transition-[opacity,scale] duration-150 ease-out active:scale-[0.98] disabled:opacity-40 focus-visible:ring-2 focus-visible:ring-(--fm-accent-bright)"
        >
          Cancel
        </button>
        <button
          type="button"
          onClick={onConfirm}
          disabled={busy || blocked}
          className="h-12 rounded-[14px] bg-(--fm-accent) text-[14px] font-semibold text-white outline-none transition-[opacity,scale] duration-150 ease-out active:scale-[0.98] disabled:opacity-60 focus-visible:ring-2 focus-visible:ring-white/70"
        >
          {busy ? working : action}
        </button>
      </div>
    </div>
  );
}

/* ── the sheet ─────────────────────────────────────────────── */

export default function FeedManageSheet({ open, scope, intent = null, onClose }: FeedManageSheetProps) {
  const manage = useFeedManage();
  const { play } = useAppHaptics();
  const reduce = Boolean(useReducedMotion());
  const isClient = useSyncExternalStore(subscribeNothing, onClient, onServer);
  const titleId = useId();
  const rootRef = useRef<HTMLDivElement | null>(null);
  const panelRef = useRef<HTMLElement | null>(null);
  const cancelRef = useRef<HTMLButtonElement | null>(null);

  const [confirm, setConfirm] = useState<'delete' | 'remove' | null>(null);
  // a delete or remove has been confirmed: the sheet holds what it shows until it is gone
  const [leaving, setLeaving] = useState(false);
  const [handleDraft, setHandleDraft] = useState('');
  const [lastAdded, setLastAdded] = useState<string | null>(null);
  const [fundRequested, setFundRequested] = useState(false);
  const [toastText, setToastText] = useState<string | null>(null);

  /* a session runs from open to close. Opened with the create intent, the sheet waits behind the Brief and only
     rises once the feed exists; closing the Brief before that closes the whole thing */
  const [session, setSession] = useState({ open: false, viaIntent: false });
  let current = session;
  if (session.open !== open) {
    current = { open, viaIntent: open && intent === 'create-feed' };
    setSession(current);
    if (open) {
      setConfirm(null);
      setLeaving(false);
      setHandleDraft('');
      setLastAdded(null);
      manage.clearError();
      if (current.viaIntent) manage.create.start();
    } else {
      manage.dismissAll();
    }
  }
  const panelOpen = open && !current.viaIntent;

  /* on the page from the moment it opens until its slide out has finished; `shown` is the slide */
  const [mounted, setMounted] = useState(false);
  const [shown, setShown] = useState(false);
  const [panelWasOpen, setPanelWasOpen] = useState(false);
  if (panelWasOpen !== panelOpen) {
    setPanelWasOpen(panelOpen);
    if (panelOpen) setMounted(true);
    else setShown(false);
  }

  /* what the sheet shows: the live scope while it is open, the last one while it closes or while what it shows
     is being deleted (the lists and the scope move under it; the sheet should not) */
  const resolved = resolveScope(scope, manage.feeds);
  const holding = !panelOpen || leaving;
  const [held, setHeld] = useState<Resolved>(resolved);
  if (!holding && (held.view !== resolved.view || held.feed !== resolved.feed || held.feeder !== resolved.feeder)) {
    setHeld(resolved);
  }
  const { view, feed, feeder } = holding ? held : resolved;
  const activeConfirm = confirm === 'delete' && view === 'feed' ? 'delete' : confirm === 'remove' && view === 'feeder' ? 'remove' : null;

  const overlayOpen = manage.create.open || manage.editBrief.open || manage.exporter.open || manage.fund.open;

  // a Brief that fails to save says so above its dialog (the dialog has no place for it)
  const dialogError = (manage.errorAction === 'create' && manage.create.open) || (manage.errorAction === 'edit' && manage.editBrief.open)
    ? manage.error
    : null;
  if (dialogError && dialogError !== toastText) setToastText(dialogError);

  const tap = () => play('snapLock');
  const requestClose = () => {
    tap();
    onClose();
  };

  /* in: on the page at rest below the fold first, then told to rise, so the rise is a transition */
  useLayoutEffect(() => {
    if (!isClient || !panelOpen || !mounted || shown) return undefined;
    panelRef.current?.getBoundingClientRect();
    const frame = window.requestAnimationFrame(() => setShown(true));
    return () => window.cancelAnimationFrame(frame);
  }, [isClient, panelOpen, mounted, shown]);

  /* out: off the page once the slide has finished */
  useEffect(() => {
    if (panelOpen || !mounted) return undefined;
    const timer = window.setTimeout(() => setMounted(false), CLOSE_MS + 60);
    return () => window.clearTimeout(timer);
  }, [panelOpen, mounted]);

  /* focus: into the sheet when it opens, back to whatever opened it when it closes */
  useEffect(() => {
    if (!panelOpen) return undefined;
    const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    const frame = window.requestAnimationFrame(() => panelRef.current?.focus({ preventScroll: true }));
    return () => {
      window.cancelAnimationFrame(frame);
      if (previous && previous !== document.body && previous.isConnected) previous.focus({ preventScroll: true });
    };
  }, [panelOpen]);

  // back from a dialog that sat above it (the sheet was inert meanwhile), focus comes back to the sheet
  const overlayWasOpen = useRef(false);
  useEffect(() => {
    const wasOpen = overlayWasOpen.current;
    overlayWasOpen.current = overlayOpen;
    if (!wasOpen || overlayOpen || !panelOpen) return undefined;
    const frame = window.requestAnimationFrame(() => panelRef.current?.focus({ preventScroll: true }));
    return () => window.cancelAnimationFrame(frame);
  }, [overlayOpen, panelOpen]);

  // the confirm step opens on its safe answer
  useEffect(() => {
    if (!activeConfirm) return undefined;
    const frame = window.requestAnimationFrame(() => cancelRef.current?.focus({ preventScroll: true }));
    return () => window.cancelAnimationFrame(frame);
  }, [activeConfirm]);

  /* keys: Esc backs out of a confirm, then closes; Tab stays inside. A dialog open above answers its own keys */
  useEffect(() => {
    if (!panelOpen) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (overlayOpen) return;
      if (event.key === 'Escape') {
        event.preventDefault();
        if (activeConfirm && !leaving) {
          setConfirm(null);
          panelRef.current?.focus({ preventScroll: true });
          return;
        }
        play('snapLock');
        onClose();
        return;
      }
      if (event.key === 'Tab') trapTab(event, panelRef.current);
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [activeConfirm, leaving, onClose, overlayOpen, panelOpen, play]);

  /* the page under the sheet doesn't scroll: wheel and touch only move the sheet's own list, when it has more */
  useEffect(() => {
    const root = rootRef.current;
    if (!mounted || !root) return undefined;
    const guard = (event: Event) => {
      const target = event.target instanceof Element ? event.target : null;
      const scroller = target?.closest<HTMLElement>('[data-ms-scroll]');
      if (scroller && scroller.scrollHeight > scroller.clientHeight + 1) return;
      if (event.cancelable) event.preventDefault();
    };
    root.addEventListener('wheel', guard, { passive: false });
    root.addEventListener('touchmove', guard, { passive: false });
    return () => {
      root.removeEventListener('wheel', guard);
      root.removeEventListener('touchmove', guard);
    };
  }, [mounted, isClient]);

  // the plan's code warms up while the all-feeds sheet is open
  useEffect(() => {
    if (!panelOpen || view !== 'all') return;
    loadFundPanel().catch(() => {});
  }, [panelOpen, view]);

  /* ── actions ── */

  const startCreate = () => {
    tap();
    manage.clearError();
    manage.create.start();
  };

  const closeCreate = () => {
    if (!manage.create.close()) return;
    if (current.viaIntent) onClose();
  };

  const saveCreate = async () => {
    const outcome = await manage.create.primary();
    if (outcome.kind !== 'created') return;
    play('snapLock');
    // the new feed is where its first feeders get added: the tab moves to it, the sheet rises on it
    if (outcome.feedId) setTabScope({ feedId: outcome.feedId, handle: null });
    setSession((value) => (value.open && value.viaIntent ? { ...value, viaIntent: false } : value));
  };

  const openFund = () => {
    tap();
    setFundRequested(true);
    manage.fund.show();
  };

  const pickFeed = (feedId: string) => {
    tap();
    setTabScope({ feedId, handle: null });
    onClose();
  };

  const submitHandle = async (feedId: string) => {
    const clean = cleanHandle(handleDraft);
    if (!clean || manage.busy) return;
    tap();
    const ok = await manage.addFeeder(feedId, clean);
    if (ok) {
      setHandleDraft('');
      setLastAdded(clean);
    }
  };

  const askConfirm = (kind: 'delete' | 'remove') => {
    tap();
    manage.clearError();
    setConfirm(kind);
  };

  const cancelConfirm = () => {
    tap();
    manage.clearError();
    setConfirm(null);
    panelRef.current?.focus({ preventScroll: true });
  };

  const confirmDelete = async (feedId: string) => {
    if (leaving || manage.busy) return;
    play('snapLock');
    setLeaving(true);
    const ok = await manage.deleteFeed(feedId);
    if (ok) onClose();
    else setLeaving(false);
  };

  const confirmRemove = async (feedId: string, handle: string) => {
    if (leaving || manage.busy) return;
    play('snapLock');
    setLeaving(true);
    const ok = await manage.removeFeeder(feedId, handle);
    if (ok) onClose();
    else setLeaving(false);
  };

  /* ── what it says ── */

  const feeds = manage.feeds;
  const feedName = feed ? titleCase(feed.title) : '';
  const totalFeeders = (feeds ?? []).reduce((total, item) => total + item.feeders.length, 0);
  const header = view === 'feeder' && feed && feeder
    ? {
      eyebrow: 'Manage feeder',
      title: `@${feeder.handle}`,
      support: [feedName, feeder.followerCount != null ? `${formatCount(feeder.followerCount)} followers` : null, feeder.isAnchor ? 'Anchor' : null]
        .filter(Boolean)
        .join(' · '),
    }
    : view === 'feed' && feed
      ? { eyebrow: 'Manage feed', title: feedName, support: feedSupport(feed) }
      : {
        eyebrow: 'Manage',
        title: 'All feeds',
        support: feeds ? `${plural(feeds.length, 'feed')} · ${plural(totalFeeders, 'feeder')}` : 'Loading your feeds…',
      };

  const pending = manage.pending;

  /* ── the rows ── */

  let menu: ReactNode = null;
  let confirmView: ReactNode = null;

  if (view === 'feed' && feed) {
    const briefText = feedBriefTextOf(feed);
    const feederCount = feed.feeders.length;
    menu = (
      <>
        <AddFeederRow
          value={handleDraft}
          adding={pending?.action === 'add' && pending.feedId === feed.id ? pending.handle : null}
          error={manage.errorAction === 'add' ? manage.error : null}
          added={lastAdded}
          blocked={manage.busy}
          slots={manage.slots}
          onChange={(value) => {
            setHandleDraft(value);
            setLastAdded(null);
            if (manage.errorAction === 'add') manage.clearError();
          }}
          onSubmit={() => {
            void submitHandle(feed.id);
          }}
        />
        <Row
          icon={<FileText size={17} strokeWidth={2.3} />}
          label="Brief"
          support={briefText || 'Add the niche, the audience and what to see first'}
          trailing={<Chevron />}
          onClick={() => {
            tap();
            manage.editBrief.start(feed.id);
          }}
        />
        <Row
          icon={<Download size={17} strokeWidth={2.3} />}
          label="Export"
          support="Excel workbook of the feed's posts"
          trailing={<Chevron />}
          onClick={() => {
            tap();
            manage.exporter.start(feed.id, null);
          }}
        />
        <Divider />
        <Row
          tone="danger"
          icon={<Trash2 size={17} strokeWidth={2.3} />}
          label="Delete feed"
          support="Removes it and everything tracked in it"
          onClick={() => askConfirm('delete')}
        />
      </>
    );
    confirmView = (
      <ConfirmView
        title={`Delete ${feedName}?`}
        body={feederCount > 0
          ? `Its ${plural(feederCount, 'feeder')} and everything tracked for them go with it. This can't be undone.`
          : "It has no feeders yet. This can't be undone."}
        action="Delete feed"
        working="Deleting…"
        busy={leaving}
        blocked={manage.busy}
        error={manage.errorAction === 'delete' ? manage.error : null}
        cancelRef={cancelRef}
        onCancel={cancelConfirm}
        onConfirm={() => {
          void confirmDelete(feed.id);
        }}
      />
    );
  } else if (view === 'feeder' && feed && feeder) {
    const anchoring = pending?.action === 'anchor' && pending.handle === feeder.handle;
    const anchorError = manage.errorAction === 'anchor' ? manage.error : null;
    menu = (
      <>
        {feeder.isAnchor ? (
          <Row
            tone="anchor"
            icon={<Crown size={17} strokeWidth={2.3} fill="currentColor" />}
            label="Anchor"
            support={`The rest of ${feedName} is measured against them`}
            disabled
            dimWhenDisabled={false}
          />
        ) : (
          <Row
            icon={<Crown size={17} strokeWidth={2.3} />}
            label="Make anchor"
            support={anchoring ? `Making @${feeder.handle} the anchor…` : anchorError ?? `Measure the rest of ${feedName} against them`}
            alert={Boolean(anchorError) && !anchoring}
            disabled={manage.busy}
            onClick={() => {
              tap();
              void manage.makeAnchor(feed.id, feeder.handle);
            }}
          />
        )}
        <Row
          icon={<ArrowUpRight size={17} strokeWidth={2.3} />}
          label="Feeder page"
          support="Every tracked post, ranked"
          trailing={<Chevron />}
          href={`/feed/${encodeURIComponent(feed.id)}/feeder/${encodeURIComponent(feeder.handle)}`}
          onClick={() => {
            tap();
            onClose();
          }}
        />
        <Divider />
        <Row
          tone="danger"
          icon={<UserMinus size={17} strokeWidth={2.3} />}
          label="Remove feeder"
          support={`Deletes what's tracked for them in ${feedName}`}
          onClick={() => askConfirm('remove')}
        />
      </>
    );
    confirmView = (
      <ConfirmView
        title={`Remove @${feeder.handle}?`}
        body={`They leave ${feedName}, and their tracked posts in it are deleted.${feeder.isAnchor ? ` ${feedName} will have no anchor until you pick one.` : ''} This can't be undone.`}
        action="Remove"
        working="Removing…"
        busy={leaving}
        blocked={manage.busy}
        error={manage.errorAction === 'remove' ? manage.error : null}
        cancelRef={cancelRef}
        onCancel={cancelConfirm}
        onConfirm={() => {
          void confirmRemove(feed.id, feeder.handle);
        }}
      />
    );
  } else {
    menu = (
      <>
        <Row
          tone="primary"
          icon={<Plus size={18} strokeWidth={2.6} />}
          label="New feed"
          support="Name it, then a two-minute Brief"
          onClick={startCreate}
        />
        <Row
          icon={<CreditCard size={17} strokeWidth={2.3} />}
          label="Plan & slots"
          support={slotsLine(manage.slots)}
          trailing={<Chevron />}
          onClick={openFund}
        />
        <AppearanceRow onPick={tap} />
        {feeds === null ? (
          <div className="px-3 pb-2 pt-4 text-[12px] font-medium text-(--ms-text-3)">Loading your feeds…</div>
        ) : feeds.length > 0 ? (
          <>
            <div className="px-3 pb-1 pt-4 text-[10px] font-black uppercase leading-none tracking-[0.14em] text-(--ms-text-3)">
              Your feeds
            </div>
            {feeds.map((item) => (
              <Row
                key={item.id}
                media={<FeedBadge title={item.title} />}
                label={titleCase(item.title)}
                support={feedSupport(item)}
                trailing={(
                  <>
                    <FaceStack feeders={item.feeders} />
                    <Chevron />
                  </>
                )}
                onClick={() => pickFeed(item.id)}
              />
            ))}
          </>
        ) : null}
      </>
    );
  }

  /* the rows and the confirm step share one cell, so swapping them is a cross-fade and the sheet keeps its height */
  const layerStyle = (visible: boolean, from: string): CSSProperties => (reduce
    ? { opacity: visible ? 1 : 0, transition: 'opacity 160ms linear' }
    : {
      opacity: visible ? 1 : 0,
      transform: visible ? 'translateY(0px)' : from,
      transition: `opacity ${SWAP_MS}ms ${EASE_OPEN}, transform ${SWAP_MS}ms ${EASE_OPEN}`,
    });

  const body = confirmView ? (
    <div className="grid">
      <div className="[grid-area:1/1]" style={layerStyle(!activeConfirm, 'translateY(-8px)')} inert={Boolean(activeConfirm)}>
        {menu}
      </div>
      <div className="self-end [grid-area:1/1]" style={layerStyle(Boolean(activeConfirm), 'translateY(10px)')} inert={!activeConfirm}>
        {confirmView}
      </div>
    </div>
  ) : menu;

  /* ── the surface ── */

  const dimStyle: CSSProperties = {
    opacity: shown ? 1 : 0,
    transition: `opacity ${shown ? OPEN_MS : CLOSE_MS}ms ${shown ? EASE_OPEN : EASE_CLOSE}`,
  };
  const panelStyle: CSSProperties = reduce
    ? { opacity: shown ? 1 : 0, transition: `opacity ${shown ? 200 : 160}ms ease-out` }
    : {
      transform: shown ? 'translateY(0px)' : 'translateY(var(--ms-off))',
      transition: shown ? `transform ${OPEN_MS}ms ${EASE_OPEN}` : `transform ${CLOSE_MS}ms ${EASE_CLOSE}`,
      willChange: 'transform',
    };

  const sheet = mounted ? (
    <div ref={rootRef} className="fm-stream" style={{ ...ROOT_STYLE, pointerEvents: panelOpen ? 'auto' : 'none' }} inert={overlayOpen}>
      <div aria-hidden="true" className="absolute inset-0 touch-none bg-(--fm-scrim)" style={dimStyle} onClick={requestClose} />
      <div className="pointer-events-none absolute inset-0 flex flex-col items-center justify-end lg:justify-center lg:p-8">
        <section
          ref={panelRef}
          role="dialog"
          aria-modal="true"
          aria-labelledby={titleId}
          tabIndex={-1}
          className={cn(
            'pointer-events-auto relative flex w-full flex-col overflow-hidden rounded-t-[28px] border border-b-0 border-(--ms-line) bg-(--ms-sheet) text-(--ms-text) outline-none',
            'max-h-[calc(100dvh_-_env(safe-area-inset-top)_-_24px)] shadow-(--st-panel-shadow) [--ms-off:100%] sm:max-w-[560px]',
            'lg:max-h-[min(720px,calc(100dvh_-_64px))] lg:w-[460px] lg:rounded-[28px] lg:border-b lg:shadow-(--st-panel-shadow-lg) lg:[--ms-off:calc(50vh_+_50%)]',
          )}
          style={panelStyle}
        >
          <div aria-hidden="true" className="mx-auto mt-2 h-1 w-9 shrink-0 rounded-full bg-fg/20 lg:hidden" />
          <header className="flex shrink-0 items-start gap-3 px-5 pb-2 pt-3 lg:pt-5">
            {view === 'feeder' && feeder ? (
              <span className="relative mt-0.5 block h-11 w-11 shrink-0 overflow-hidden rounded-full">
                <FeederStoryAvatar feeder={feeder} className="text-[14px]" />
              </span>
            ) : null}
            <div className="min-w-0 flex-1">
              <div className="text-[10px] font-black uppercase leading-none tracking-[0.14em] text-(--ms-text-3)">{header.eyebrow}</div>
              <h2 id={titleId} className="mt-2 truncate text-[28px] font-black leading-[1.05] tracking-[-0.04em] text-(--ms-text)">
                {header.title}
              </h2>
              <p className="mt-1 truncate text-[12px] font-medium leading-4 text-(--ms-text-2)">{header.support}</p>
            </div>
            <button
              type="button"
              onClick={requestClose}
              aria-label="Close"
              className="relative -mr-1 grid h-10 w-10 shrink-0 place-items-center rounded-[14px] border border-(--ms-line) bg-(--ms-chip) text-(--ms-text-2) outline-none transition-[scale] duration-150 ease-out active:scale-[0.94] focus-visible:ring-2 focus-visible:ring-(--fm-accent-bright)"
            >
              <X size={17} strokeWidth={2.4} />
            </button>
          </header>
          <div
            data-ms-scroll=""
            className="min-h-0 flex-1 overflow-y-auto overscroll-contain px-2 pb-[calc(12px_+_env(safe-area-inset-bottom))] pt-1 lg:pb-3"
          >
            {body}
          </div>
        </section>
      </div>
    </div>
  ) : null;

  const toast = toastText ? (
    <div className="pointer-events-none fixed inset-x-0 top-[calc(12px_+_env(safe-area-inset-top))] z-[270] flex justify-center px-4">
      <div
        role={dialogError ? 'alert' : undefined}
        className="flex max-w-[440px] items-center gap-2.5 rounded-[14px] border border-white/10 bg-(--fm-ink) py-2 pl-3.5 pr-1.5 text-white shadow-[0_18px_40px_-18px_rgba(0,0,0,0.9)]"
        style={{
          opacity: dialogError ? 1 : 0,
          transform: dialogError ? 'translateY(0px)' : 'translateY(-8px)',
          transition: `opacity 240ms ${EASE_OPEN}, transform 240ms ${EASE_OPEN}`,
          pointerEvents: dialogError ? 'auto' : 'none',
        }}
      >
        <span aria-hidden="true" className="h-2 w-2 shrink-0 rounded-full bg-(--fm-accent)" />
        <span className="min-w-0 flex-1 text-[14px] font-semibold leading-5">{toastText}</span>
        <button
          type="button"
          onClick={manage.clearError}
          aria-label="Dismiss"
          className="grid h-8 w-8 shrink-0 place-items-center rounded-[10px] text-white/60 outline-none focus-visible:ring-2 focus-visible:ring-(--fm-accent-bright)"
        >
          <X size={15} strokeWidth={2.4} />
        </button>
      </div>
    </div>
  ) : null;

  if (!isClient) return null;

  return (
    <>
      {createPortal(
        <>
          {sheet}
          {/* the Brief dialog draws in place: it rides in this portal so it sits over the sheet and the nav */}
          <FeedBriefDialog
            open={open && manage.create.open}
            mode="create"
            title={manage.create.name || 'New feed'}
            feedName={manage.create.name}
            brief={manage.create.brief}
            step={manage.create.step}
            bibleDraft={manage.create.bibleDraft}
            isBusy={manage.busy}
            onFeedNameChange={manage.create.setName}
            onBriefChange={manage.create.updateBrief}
            onStepChange={manage.create.setStep}
            onBibleDraftChange={manage.create.setBibleDraft}
            onClose={closeCreate}
            onPrimary={() => {
              void saveCreate();
            }}
            onBack={manage.create.back}
            onStartOver={manage.create.startOver}
          />
          <FeedBriefDialog
            open={open && manage.editBrief.open}
            mode="edit"
            title={manage.editBrief.feed?.title || 'Feed'}
            brief={manage.editBrief.brief}
            step={manage.editBrief.step}
            bibleDraft={manage.editBrief.bibleDraft}
            isBusy={manage.busy}
            onBriefChange={manage.editBrief.updateBrief}
            onStepChange={manage.editBrief.setStep}
            onBibleDraftChange={manage.editBrief.setBibleDraft}
            onClose={() => {
              manage.editBrief.close();
            }}
            onPrimary={() => {
              void manage.editBrief.primary();
            }}
            onBack={manage.editBrief.back}
            onStartOver={manage.editBrief.startOver}
          />
          {toast}
        </>,
        document.body,
      )}
      <FeedExportDialog
        open={open && manage.exporter.open}
        scopeLabel={manage.exporter.scopeLabel}
        from={manage.exporter.from}
        to={manage.exporter.to}
        fields={manage.exporter.fields}
        includeSummary={manage.exporter.includeSummary}
        isExporting={manage.exporter.isExporting}
        error={manage.exporter.error}
        onFromChange={manage.exporter.setFrom}
        onToChange={manage.exporter.setTo}
        onFieldsChange={manage.exporter.setFields}
        onIncludeSummaryChange={manage.exporter.setIncludeSummary}
        onClose={manage.exporter.close}
        onExport={() => {
          void manage.exporter.download();
        }}
      />
      {fundRequested ? <FundPanelOverlay open={open && manage.fund.open} onClose={manage.fund.hide} /> : null}
    </>
  );
}
