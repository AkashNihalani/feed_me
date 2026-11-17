'use client';

/* ─────────────────────────────────────────────────────────────
   THE RAIL — the files on one row of bold cards, under two tabs
   (Following, Everything) set as type, a short ink rule
   under the chosen one. Every card in every tab has one height at
   rest and one while a file is open, so a change of tab never moves
   the page. While a file is open it docks under the top of the
   screen, so another file, or the open one tapped again to close
   it, is always one tap away. A row moves by
   finger, trackpad or its two paging buttons (mouse), which go when
   there is nothing further their way.
   ───────────────────────────────────────────────────────────── */

import { memo, useEffect, useLayoutEffect, useRef, useState } from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { ms } from '@/lib/tempo';
import { cn } from '@/lib/utils';
import { Card, type CardData } from './Words';
import { WALL_MS } from './Feed';

export type ShelfData = { key: string; name: string; cards: CardData[] };

// what each tab holds, said under it
const HOLDS: Record<string, string> = {
  following: 'Files you follow',
  recent: 'Files that got new posts in this drop',
  everything: 'Every piece, person, product and place the reader has tagged',
};

export const Rail = memo(function Rail({ shelves, sel, onOpen, phone, open }: { shelves: ShelfData[]; sel: string | null; onOpen: (key: string) => void; phone: boolean; open: boolean }) {
  const shown = shelves.filter((s) => s.cards.length);
  const [tab, setTab] = useState<string>(() => shown[0]?.key ?? 'following');
  // the open card's tab is the one showing (a card that is in the tab already on show never moves the tabs)
  const here = shown.find((s) => s.key === tab) ?? shown[0];
  const selTab = sel ? (here?.cards.some((c) => c.key === sel) ? here.key : shown.find((s) => s.cards.some((c) => c.key === sel))?.key) : undefined;
  const [lastSel, setLastSel] = useState(sel);
  if (sel !== lastSel) {
    setLastSel(sel);
    if (selTab && selTab !== tab) setTab(selTab);
  }
  const current = shown.find((s) => s.key === tab) ?? shown[0];
  const ref = useRef<HTMLDivElement | null>(null);
  const railRef = useRef<HTMLDivElement | null>(null);
  /* a phone at rest shows the tabs alone under the search (the strip of cards is shut); a tap on a tab opens the strip
     and brings the rail to the top of the screen, the wall filling what is under it. A file open always has it open */
  const [stripOpen, setStripOpen] = useState(false);
  const strip = !phone || open || stripOpen;
  const pickTab = (key: string) => {
    setTab(key);
    if (!phone) return;
    setStripOpen(true);
    requestAnimationFrame(() => {
      const rail = railRef.current;
      if (!rail) return;
      const y = rail.getBoundingClientRect().top + window.scrollY;
      if (Math.abs(window.scrollY - y) < 2) return;
      const still = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
      window.scrollTo({ top: y, behavior: still ? 'auto' : 'smooth' });
    });
  };
  const tabsRef = useRef<HTMLDivElement | null>(null);

  /* the one moving part of the tabs: a short ink rule under the chosen name, gliding to the next and taking its width
     (a transform: it moves and scales, measured against the row itself, never offsetLeft, and again whenever the row
     or a name changes size, a font arriving) */
  const ruleRef = useRef<HTMLSpanElement | null>(null);
  const placed = useRef(false);
  useLayoutEffect(() => {
    const track = tabsRef.current;
    const rule = ruleRef.current;
    if (!track || !rule) return undefined;
    const put = (glide: boolean) => {
      const n = track.querySelector<HTMLElement>(`[data-tab="${current?.key}"] .rn-tab__n`);
      if (!n) return;
      const a = track.getBoundingClientRect();
      const b = n.getBoundingClientRect();
      if (!glide) rule.style.transition = 'none';
      rule.style.transform = `translate3d(${b.left - a.left}px, 0, 0) scaleX(${b.width / 100})`;
      if (!glide) {
        void rule.getBoundingClientRect();
        rule.style.transition = '';
      }
    };
    put(placed.current);
    placed.current = true;
    // (a change of the row's or a name's size only: the observer's first call, on observing, is not one)
    const names = [...track.querySelectorAll<HTMLElement>('.rn-tab__n')];
    const sizes = () => [track, ...names].map((el) => Math.round(el.getBoundingClientRect().width)).join();
    let was = sizes();
    const ro = new ResizeObserver(() => {
      const now = sizes();
      if (now === was) return;
      was = now;
      put(false);
    });
    [track, ...names].forEach((el) => ro.observe(el));
    return () => ro.disconnect();
  }, [current?.key, shown.length, phone]);
  // the arrows walk the tabs (the chosen one is the one reached by Tab)
  const onTabKey = (e: React.KeyboardEvent<HTMLButtonElement>) => {
    if (e.key !== 'ArrowRight' && e.key !== 'ArrowLeft') return;
    e.preventDefault();
    const i = shown.findIndex((x) => x.key === current?.key);
    const next = shown[(i + (e.key === 'ArrowRight' ? 1 : shown.length - 1)) % shown.length];
    if (!next) return;
    setTab(next.key);
    tabsRef.current?.querySelector<HTMLElement>(`[data-tab="${next.key}"]`)?.focus();
  };

  /* the paging buttons go when there is nothing further their way */
  useEffect(() => {
    const el = ref.current;
    if (!el) return undefined;
    el.scrollLeft = 0;
    let raf = 0;
    const put = () => {
      raf = 0;
      rowLeft.current = el.scrollLeft;
      const box = el.parentElement;
      box?.classList.toggle('is-start', el.scrollLeft <= 2);
      box?.classList.toggle('is-end', el.scrollLeft >= el.scrollWidth - el.clientWidth - 2);
    };
    const on = () => {
      if (!raf) raf = requestAnimationFrame(put);
    };
    put();
    el.addEventListener('scroll', on, { passive: true });
    window.addEventListener('resize', on);
    return () => {
      cancelAnimationFrame(raf);
      el.removeEventListener('scroll', on);
      window.removeEventListener('resize', on);
    };
  }, [current?.key, current?.cards.length, strip]);
  // the open card is brought into the row's view
  useEffect(() => {
    const el = ref.current;
    const card = sel ? el?.querySelector<HTMLElement>(`.rn-card[data-key="${CSS.escape(sel)}"]`) : null;
    if (!el || !card) return;
    // (measured against the row itself: Safari counts the card's transformed wrapper as its offsetParent, so
    // offsetLeft there is 0 and the row would glide back to its start)
    const item = card.parentElement ?? card;
    const x = item.getBoundingClientRect().left - el.getBoundingClientRect().left + el.scrollLeft;
    const l = x - 16;
    const r = x + item.offsetWidth + 16 - el.clientWidth;
    if (l < el.scrollLeft) el.scrollTo({ left: l, behavior: 'smooth' });
    else if (r > el.scrollLeft) el.scrollTo({ left: r, behavior: 'smooth' });
  }, [sel]);
  /* THE FOLD. While a file is open the cards are short (figure and title: the docked switcher); at rest they carry
     this drop's line as well. The layout takes its new height at once (the wall measures from it), and what is seen
     plays out over it: each card's face glides from its old height to its new one, the line and the foot go out as
     the face closes over them (or come in as it opens), on the wall's own curve. */
  // where the row stands, kept so a fold never moves it (a phone's browser may jump a row back to its start when
  // what is inside it changes size)
  const rowLeft = useRef(0);
  const sizes = useRef(new Map<string, { h: number; line: [number, number, number] | null; foot: [number, number, number] | null }>());
  const wasOpen = useRef(open);
  useLayoutEffect(() => {
    const row = ref.current;
    if (!row) return;
    const cards = [...row.querySelectorAll<HTMLElement>('.rn-card')];
    const changed = open !== wasOpen.current;
    wasOpen.current = open;
    if (changed) {
      const keep = rowLeft.current;
      const hold = () => {
        if (Math.abs(row.scrollLeft - keep) > 1) row.scrollLeft = keep;
      };
      hold();
      requestAnimationFrame(() => {
        hold();
        requestAnimationFrame(hold);
      });
    }
    if (changed && !window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      const box = row.getBoundingClientRect();
      // (the wall's own beat and its tiles' travel curve, so the row and the wall move as one)
      const dur = ms(WALL_MS);
      const easing = 'cubic-bezier(0.42, 0, 0.1, 1)';
      let most = 0;
      const cleanup: (() => void)[] = [];
      cards.forEach((card) => {
        const was = sizes.current.get(card.dataset.key ?? '');
        const r = card.getBoundingClientRect();
        if (!was || r.right < box.left || r.left > box.right) return;
        const dh = was.h - card.offsetHeight;
        if (Math.abs(dh) < 1) return;
        most = Math.max(most, dh);
        // the face (and the ring round an open card) from the old height to the new
        card.animate([{ bottom: `${-dh}px` }, { bottom: '0px' }], { duration: dur, easing, pseudoElement: '::before' });
        card.animate([{ bottom: `${-3 - dh}px` }, { bottom: '-3px' }], { duration: dur, easing, pseudoElement: '::after' });
        const words = [
          [card.querySelector<HTMLElement>('.rn-card__l'), was.line],
          [card.querySelector<HTMLElement>('.rn-card__foot'), was.foot],
        ] as const;
        words.forEach(([el, at]) => {
          if (!el) return;
          if (dh > 0 && at) {
            // closing over them: they stay where they were, out of the layout, and go
            el.style.cssText = `display:${el.classList.contains('rn-card__foot') ? 'flex' : '-webkit-box'};position:absolute;left:${at[0]}px;top:${at[1]}px;width:${at[2]}px;margin:0;pointer-events:none`;
            const a = el.animate([{ opacity: 1, transform: 'translate3d(0,0,0)' }, { opacity: 0, transform: 'translate3d(0,-10px,0)' }], { duration: Math.round(dur * 0.5), easing: 'cubic-bezier(0.4, 0, 1, 1)', fill: 'forwards' });
            cleanup.push(() => {
              a.cancel();
              el.style.cssText = '';
            });
          } else if (dh < 0) {
            el.animate([{ opacity: 0, transform: 'translate3d(0,-10px,0)' }, { opacity: 1, transform: 'translate3d(0,0,0)' }], { duration: Math.round(dur * 0.6), delay: Math.round(dur * 0.36), easing: 'cubic-bezier(0.16, 1, 0.3, 1)', fill: 'backwards' });
          }
        });
      });
      if (most > 0) {
        // (the row clips what hangs under it: for the fold it is given the room, without changing its height in the page)
        const pad = parseFloat(getComputedStyle(row).paddingBottom) || 0;
        row.style.paddingBottom = `${pad + most}px`;
        row.style.marginBottom = `${-most}px`;
        cleanup.push(() => {
          row.style.paddingBottom = '';
          row.style.marginBottom = '';
        });
      }
      if (cleanup.length) window.setTimeout(() => cleanup.forEach((f) => f()), dur + 30);
    }
    // what is drawn now, for the next fold
    cards.forEach((card) => {
      const l = card.querySelector<HTMLElement>('.rn-card__l');
      const f = card.querySelector<HTMLElement>('.rn-card__foot');
      const at = (el: HTMLElement | null): [number, number, number] | null => (el && el.offsetParent ? [el.offsetLeft, el.offsetTop, el.offsetWidth] : null);
      const prev = sizes.current.get(card.dataset.key ?? '');
      sizes.current.set(card.dataset.key ?? '', { h: card.offsetHeight, line: at(l) ?? (open ? (prev?.line ?? null) : null), foot: at(f) ?? (open ? (prev?.foot ?? null) : null) });
    });
  });

  const page = (d: 1 | -1) => {
    const el = ref.current;
    if (!el) return;
    const card = el.querySelector<HTMLElement>('.rn-card');
    el.scrollBy({ left: d * Math.max(240, el.clientWidth - (card?.offsetWidth ?? 240)), behavior: 'smooth' });
  };
  if (!current) return null;
  return (
    <div className={cn('rn-rail', !strip && 'is-shut')} ref={railRef}>
      <div className="rn-tabhead">
        <div className="rn-tabs" role="tablist" aria-label="Files" ref={tabsRef}>
          {shown.map((s) => (
            <button key={s.key} type="button" role="tab" className="rn-tab" data-tab={s.key} aria-selected={s.key === current.key} tabIndex={s.key === current.key ? 0 : -1} onClick={() => pickTab(s.key)} onKeyDown={onTabKey}>
              <span className="rn-tab__n">{s.name}</span>
              <sup>{s.cards.length}</sup>
            </button>
          ))}
          <span className="rn-tabs__rule" ref={ruleRef} aria-hidden="true" />
        </div>
        <p className="rn-tabs__say" key={current.key}>
          {HOLDS[current.key] ?? ''}
        </p>
      </div>
      {strip ? (
      <div className="rn-rail__box is-start">
        <div className="rn-rail__row" ref={ref} role="tabpanel" key={current.key}>
          {current.cards.map((c, i) => (
            <div key={c.key} className="rn-rail__i" style={{ ['--i' as string]: Math.min(i, 8) }}>
              <Card card={c} sel={sel === c.key} onOpen={onOpen} />
            </div>
          ))}
        </div>
        <button type="button" className="rn-page is-l" onClick={() => page(-1)} aria-label="Earlier cards">
          <ChevronLeft aria-hidden="true" strokeWidth={2.6} />
        </button>
        <button type="button" className="rn-page is-r" onClick={() => page(1)} aria-label="More cards">
          <ChevronRight aria-hidden="true" strokeWidth={2.6} />
        </button>
      </div>
      ) : null}
    </div>
  );
});
