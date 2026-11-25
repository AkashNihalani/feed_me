'use client';

/* ─────────────────────────────────────────────────────────────
   ME, at the door — how many Bitecoins are left, beside the mark,
   wherever Me can be opened from: the line over the dock's tabs on
   a phone (DockMe), the foot of the side rail from a laptop up
   (RailMe). It stands apart from the three tabs: it is smaller, and
   the only thing in the nav with a number.
   ───────────────────────────────────────────────────────────── */

import { memo, useEffect, useRef, useState } from 'react';
import { UserRound } from 'lucide-react';
import { bitecoinsLabel, useBitecoins } from '@/lib/bitecoins';
import { openMe } from '@/lib/meDoor';
import { aModalIsOpen, typingIn } from '@/lib/keys';
import { leaveTab } from '@/lib/tabSheet';
import { ms } from '@/lib/tempo';
import { cn } from '@/lib/utils';

// the mark, its mouth open this far (degrees), on a 20-unit square
function markPath(deg: number) {
  const R = 10;
  const a = Math.max(0.004, (deg * Math.PI) / 180);
  const tip = Math.min(R * 0.06, R * a * 0.5);
  const da = Math.asin(Math.min(0.9, tip / (R - tip))) + a;
  const ax = (R - tip) * Math.cos(da);
  const ay = (R - tip) * Math.sin(da);
  const bx = (R - tip) * Math.cos(da - a);
  const f = (value: number) => value.toFixed(2);
  const lip = [ax + tip * Math.cos(a - Math.PI / 2), ay + tip * Math.sin(a - Math.PI / 2)];
  const end = [R * Math.cos(da), R * Math.sin(da)];
  return `M ${f(R * 0.02)} 0 L ${f(Math.cos(a) * bx)} ${f(Math.sin(a) * bx)} L ${f(lip[0])} ${f(lip[1])} A ${f(tip)} ${f(tip)} 0 0 1 ${f(end[0])} ${f(end[1])} A ${R} ${R} 0 1 1 ${f(end[0])} ${f(-end[1])} A ${f(tip)} ${f(tip)} 0 0 1 ${f(lip[0])} ${f(-lip[1])} L ${f(Math.cos(a) * bx)} ${f(-Math.sin(a) * bx)} Z`;
}
const OPEN = 33;
const AT_REST = markPath(OPEN);

function Mark({ size, hello = false }: { size: number; hello?: boolean }) {
  const path = useRef<SVGPathElement | null>(null);
  // it says hello as the layers come up: shuts and opens, once
  useEffect(() => {
    const el = path.current;
    if (!hello || !el || window.matchMedia('(prefers-reduced-motion: reduce)').matches) return undefined;
    let raf = 0;
    const start = performance.now() + ms(310);
    const step = (now: number) => {
      const t = now - start;
      const shut = Math.min(1, Math.max(0, t / 95));
      const open = Math.min(1, Math.max(0, (t - 95) / 260));
      const out = (k: number) => 1 - (1 - k) ** 3;
      el.setAttribute('d', markPath(OPEN - 30 * out(shut) + 30 * out(open)));
      if (open < 1) raf = requestAnimationFrame(step);
      else el.setAttribute('d', AT_REST);
    };
    raf = requestAnimationFrame(step);
    return () => {
      cancelAnimationFrame(raf);
      el.setAttribute('d', AT_REST);
    };
  }, [hello]);
  return (
    <svg aria-hidden="true" viewBox="-10 -10 20 20" width={size} height={size} className="block shrink-0">
      <path ref={path} d={AT_REST} fill="#DA2E46" />
    </svg>
  );
}

export const DockMe = memo(function DockMe({ on, open }: { on: boolean; open: boolean }) {
  const balance = useBitecoins();
  const label = bitecoinsLabel(balance);
  return (
    <div className="fm-dock-me">
      <button type="button" data-pick="me" className={on ? 'is-on' : undefined} aria-label={`Me, ${label}`} aria-current={on ? 'page' : undefined}>
        <b>Me</b>
        <span>
          <Mark size={14} hello={open} />
          <small>{label}</small>
        </span>
      </button>
    </div>
  );
});

export function RailMe({ tab, open, iconBox }: { tab: string | null; open: boolean; iconBox: string }) {
  const balance = useBitecoins();
  const on = tab === 'me';
  // the tile answers the click at once, as the tabs' does
  const [going, setGoing] = useState<string | null>(null);
  if (going && going !== tab) setGoing(null);
  const lit = on || Boolean(going);
  const go = useRef(() => {});
  useEffect(() => {
    go.current = () => {
      if (on) {
        window.scrollTo({ top: 0, behavior: 'smooth' });
        return;
      }
      if (tab) setGoing(tab);
      void leaveTab(tab || 'feed', 'me', 1).then(openMe);
    };
  });
  // M, as F, L and R are the tabs' (not while typing, or under a dialog on screen)
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey || event.shiftKey || event.repeat || event.key.toLowerCase() !== 'm') return;
      if (typingIn(event.target) || aModalIsOpen()) return;
      go.current();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
  const label = bitecoinsLabel(balance);
  return (
    <a
      href="/me"
      onClick={(event) => {
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        event.preventDefault();
        go.current();
      }}
      aria-current={on ? 'page' : undefined}
      aria-label={`Me, ${label} (M)`}
      className={cn('group/row relative flex h-11 items-center gap-4 rounded-[16px] pl-2 outline-none transition-colors duration-200 hover:text-fg focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]', lit ? 'text-fg' : 'text-fg/62')}
    >
      <span data-rail-mag="" className={cn(iconBox, 'relative')}>
        <span className={cn('grid h-11 w-11 place-items-center rounded-[14px] transition-[background-color,color,box-shadow] duration-200', lit && 'bg-fg text-(--fm-page) shadow-[0_10px_22px_-10px_rgb(var(--fm-shadow-rgb)/0.55)] dark:shadow-[0_10px_24px_-8px_rgba(0,0,0,0.8)]')}>
          <UserRound aria-hidden="true" size={20} strokeWidth={2.2} />
        </span>
      </span>
      <span className="fm-rail-name relative grid min-w-0 gap-1">
        <span className="truncate text-[17px] font-bold leading-none tracking-[-0.02em] text-fg">Me</span>
        <span className="flex items-center gap-[5px] text-[14px] font-medium leading-4 tracking-[-0.01em] text-fg/66 tabular-nums">
          <Mark size={13} hello={open} />
          {label}
        </span>
      </span>
    </a>
  );
}
