'use client';

/* ─────────────────────────────────────────────────────────────
   ASK — the hero. One raised slab, full width, built of layered
   shadows (contact, ambient, a faint crimson beneath, a light rim),
   the login's streak of light turning round its edge. Four states:

   - rest: the questions the reader raises from this drop roll in
     the big type, one at a time, each with a small line saying why
     it is raised, and a token saying what taking it costs (a search
     spends bites, opening is free). The send disc takes the one
     showing. With none, the line is one still invitation and the
     disc idles. A tap anywhere else on the box means type.
   - typing: the box folds to a compact bar at the top of what is
     visible (the page glides it there), so the keyboard never
     covers it, and the rows under the field are sized to the
     visible area. Nothing typed: one quiet line. Typing: what the
     reader already knows that matches (files, people, products,
     places), each opening free, and last, always, the real search.
     Esc, a press on the veil, Safari's Done or Back closes it.
   - scanning: the question holds the line, a count says how far the
     reader is through his feed, a hairline fills along the box's
     foot. The disc stops it (so does Back).
   - answered: the question stays, with what it cost. The disc
     clears it (so does Back); a tap on the box types a new one.
   ───────────────────────────────────────────────────────────── */

import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, type KeyboardEvent } from 'react';
import { ArrowRight, ArrowUp, X } from 'lucide-react';
import { cn } from '@/lib/utils';
import Streak from './Streak';
import { ms } from '@/lib/tempo';

export type AskPhase = 'rest' | 'scan' | 'answer';
/* a lead the box offers: the reader's question, why it is raised, and what taking it does (a search, or a row or file
   it opens free) */
export type AskLead = { key: string; text: string; sub: string; to: string; search: boolean; kind?: string };
/* something the reader already knows, matched as the person types */
export type AskMatch = { key: string; name: string; alt: string | null; kind: string; cover: string | null; posts: number; landed: number };
export type AskSearch = { text: string; ok: boolean; reason: string | null };

const HOLD = 3400;
/* the leads type in: the last one is taken back a letter at a time, the next typed out at a person's pace (a beat longer
   after a space or a mark), and held once it is whole */
const TYPE = 42;
const ERASE = 16;
const sizeOfLead = (t: string) => (t.length > 56 ? 2 : t.length > 34 ? 1 : 0);
const SIZE_CLASS = ['', 'is-long', 'is-longer'];
/* the line the leads are typed into. It runs on its own: each letter is written straight into the text (no render of
   the box per letter), and the box hears only when a sentence is whole or is being changed. The whole sentence is always
   laid in the line, its untyped part invisible, so nothing reflows and the size holds; a sentence set in another size is
   taken back to nothing first */
const TypedLine = memo(function TypedLine({ text, still, onWhole }: { text: string; still: boolean; onWhole: (whole: boolean) => void }) {
  const lineRef = useRef<HTMLSpanElement | null>(null);
  const typedRef = useRef<HTMLSpanElement | null>(null);
  const restRef = useRef<HTMLSpanElement | null>(null);
  const caretRef = useRef<HTMLElement | null>(null);
  // what is on screen now, and the sentence the line is laid for
  const cur = useRef(text);
  const laid = useRef(text);
  const wholeWas = useRef(true);
  const onWholeRef = useRef(onWhole);
  useEffect(() => {
    onWholeRef.current = onWhole;
  });
  useLayoutEffect(() => {
    const line = lineRef.current;
    const typedEl = typedRef.current;
    const restEl = restRef.current;
    const caret = caretRef.current;
    if (!line || !typedEl || !restEl || !caret) return undefined;
    const paint = () => {
      typedEl.textContent = cur.current;
      restEl.textContent = laid.current.slice(cur.current.length);
      const size = SIZE_CLASS[sizeOfLead(laid.current)];
      line.classList.toggle('is-long', size === 'is-long');
      line.classList.toggle('is-longer', size === 'is-longer');
      const whole = cur.current === text;
      caret.classList.toggle('is-idle', whole);
      if (whole !== wholeWas.current) {
        wholeWas.current = whole;
        onWholeRef.current(whole);
      }
    };
    let clear = sizeOfLead(laid.current) !== sizeOfLead(text);
    // (what is on screen is drawn at once, before the first letter changes: never an empty frame)
    paint();
    let t = 0;
    const step = () => {
      if (still) {
        cur.current = text;
        laid.current = text;
        return paint();
      }
      if (clear && !cur.current) clear = false;
      if (clear || !text.startsWith(cur.current)) {
        // (taken back a letter at a time, still laid as the sentence it was)
        cur.current = cur.current.slice(0, -1);
        paint();
        t = window.setTimeout(step, ms(ERASE));
        return;
      }
      laid.current = text;
      if (cur.current.length < text.length) {
        cur.current = text.slice(0, cur.current.length + 1);
        paint();
        t = window.setTimeout(step, ms(TYPE + (/[\s,.?!:]/.test(cur.current[cur.current.length - 1]) ? 50 : 0)));
      } else paint();
    };
    t = window.setTimeout(step, 0);
    return () => window.clearTimeout(t);
  }, [text, still]);
  return (
    // (its classes and its text are the line's own, written as it types; React never sets them again)
    <span ref={lineRef} className="rn-ask__ri is-typed" aria-label={text}>
      <span ref={typedRef} aria-hidden="true" />
      <i ref={caretRef} className="rn-ask__tc is-idle" aria-hidden="true" />
      <span ref={restRef} className="rn-ask__rest" aria-hidden="true" />
    </span>
  );
});
// the box's resting height (the CSS holds the same numbers), and the typing bar's parts (rows are fixed heights, so the
// slab's height is counted, never measured)
const REST_PHONE = 200;
// (a phone at rest: the box is the page's hero, the lead set large in up to four lines)
const HERO_PHONE = 236;
const REST_DESK = 160;
const FIELD_PHONE = 68;
const FIELD_DESK = 72;
const ROW = 56;
const HINT = 48;
const GO = 64;
const GO_NO = 84;
const PAD = 12;
const RIM = 4;
// the slab's foot stays this far above the bottom of what is visible (the keyboard's top, on a phone)
const FOOT = 12;

function Ask({
  leads,
  matches,
  searches,
  phase,
  question,
  answerNote,
  phone,
  handle,
  cost,
  scope,
  caught,
  focusKey,
  onTake,
  onOpen,
  onSend,
  onClear,
  onStop,
  onTyping,
  onLift,
}: {
  leads: AskLead[];
  matches: AskMatch[];
  searches: AskSearch[];
  phase: AskPhase;
  question: string;
  // under an answered question: what it cost
  answerNote: string;
  phone: boolean;
  // the feeder's handle (a phone's hero names it)
  handle?: string;
  cost: number;
  // how many posts a search reads
  scope: number;
  // the posts the scan has caught so far: a slot each, which the wall's picture flies to
  caught: { id: string; cover: string }[];
  focusKey: number;
  onTake: (lead: AskLead) => void;
  onOpen: (key: string) => void;
  onSend: (text: string) => void;
  onClear: () => void;
  onStop: () => void;
  onTyping: (on: boolean) => void;
  // the box comes forward (the page recedes behind it) for as long as it is typing
  onLift?: (on: boolean) => void;
}) {
  const [typing, setTyping] = useState(false);
  const [text, setText] = useState('');
  const [active, setActive] = useState(-1);
  const [turn, setTurn] = useState(0);
  // the room from the box's top to the visible bottom, once the page has glided the box up
  const [room, setRoom] = useState(0);
  const boxRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLInputElement | null>(null);
  const glide = useRef<number | null>(null);

  /* ── the leads: one rolls every 3.4s while the box is at rest and not in use (a new drop's leads start over, settled
     while rendering) ── */
  const [seen, setSeen] = useState(leads);
  if (seen !== leads) {
    setSeen(leads);
    setTurn(0);
  }
  const rolling = phase === 'rest' && !typing;
  useEffect(() => {
    if (!rolling || leads.length < 2) return undefined;
    const cur = leads[turn % leads.length]?.text.length ?? 0;
    const was = turn > 0 ? (leads[(turn - 1) % leads.length]?.text.length ?? 0) : 0;
    const id = window.setTimeout(() => !document.hidden && setTurn((x) => x + 1), ms(was * ERASE + cur * (TYPE + 8)) + HOLD - 800);
    return () => window.clearTimeout(id);
  }, [rolling, leads, turn]);
  const shown = leads.length ? leads[turn % leads.length] : null;
  const still = typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  // (the line tells the box only when its sentence is whole, or no longer is: the note under it waits for that)
  const [whole, setWhole] = useState(true);
  // the lead before it, rolling out as it rolls in
  const [roll, setRoll] = useState<{ cur: AskLead | null; prev: AskLead | null; k: number }>({ cur: shown, prev: null, k: 0 });
  if (roll.cur?.key !== shown?.key) setRoll({ cur: shown, prev: roll.cur, k: roll.k + 1 });

  /* ── typing: what matches, best first (starts with, then a word starting with it, then contains) ── */
  const q = text.trim().toLowerCase();
  const found = useMemo(() => {
    if (!q) return [];
    const score = (m: AskMatch) => {
      const name = m.name.toLowerCase();
      if (name.startsWith(q)) return 0;
      if (name.split(/\s+/).some((w) => w.startsWith(q))) return 1;
      if (name.includes(q)) return 2;
      if (m.alt && m.alt.toLowerCase().includes(q)) return 3;
      return -1;
    };
    return matches
      .map((m) => ({ m, s: score(m) }))
      .filter((x) => x.s >= 0)
      .sort((a, b) => a.s - b.s || b.m.posts - a.m.posts)
      .map((x) => x.m);
  }, [q, matches]);
  const refused = q ? searches.find((s) => !s.ok && s.text.toLowerCase() === q) ?? null : null;
  const field = phone ? FIELD_PHONE : FIELD_DESK;
  const goH = refused ? GO_NO : GO;
  const fit = Math.max(0, Math.min(phone ? 4 : 5, Math.floor((room - field - PAD - RIM - goH) / ROW)));
  const rows = found.slice(0, fit);

  /* the room to the visible bottom: the visual viewport's foot (above the keyboard) less the box's top, the top it will
     have once the page's glide ends. Read on open and whenever the visible area or the page moves */
  const measure = useCallback(() => {
    const box = boxRef.current;
    if (!box) return;
    const vv = window.visualViewport;
    const bottom = vv ? vv.offsetTop + vv.height : window.innerHeight;
    const top = box.getBoundingClientRect().top + (glide.current != null ? window.scrollY - glide.current : 0);
    setRoom(Math.max(0, Math.floor(bottom - FOOT - top)));
  }, []);
  useEffect(() => {
    if (!typing) return undefined;
    const box = boxRef.current;
    const root = box?.closest<HTMLElement>('.rn');
    if (box && root) {
      // glide the page so the box sits where it sits at the page's top, just under the status bar
      const pad = parseFloat(getComputedStyle(root).paddingTop) || 0;
      const target = Math.max(0, Math.round(window.scrollY + box.getBoundingClientRect().top - pad));
      if (Math.abs(target - window.scrollY) > 2) {
        glide.current = target;
        const still = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        window.scrollTo({ top: target, behavior: still ? 'auto' : 'smooth' });
      }
    }
    measure();
    let end = 0;
    const onScroll = () => {
      if (glide.current != null && Math.abs(window.scrollY - glide.current) < 2) glide.current = null;
      window.clearTimeout(end);
      end = window.setTimeout(() => {
        glide.current = null;
        measure();
      }, 160);
      measure();
    };
    const vv = window.visualViewport;
    vv?.addEventListener('resize', measure);
    vv?.addEventListener('scroll', measure);
    window.addEventListener('scroll', onScroll, { passive: true });
    window.addEventListener('resize', measure);
    return () => {
      window.clearTimeout(end);
      glide.current = null;
      vv?.removeEventListener('resize', measure);
      vv?.removeEventListener('scroll', measure);
      window.removeEventListener('scroll', onScroll);
      window.removeEventListener('resize', measure);
    };
  }, [typing, measure]);

  /* the slab's height: at rest its resting height; typing, the field row and the rows under it (counted), never past the
     visible bottom. One transition on the app's tempo carries it both ways */
  useLayoutEffect(() => {
    const box = boxRef.current;
    if (!box) return;
    const rest = phone ? (phase === 'rest' ? HERO_PHONE : REST_PHONE) : REST_DESK;
    if (!typing) {
      box.style.height = `${rest}px`;
      return;
    }
    const body = q ? rows.length * ROW + goH : HINT;
    const h = field + body + PAD + RIM;
    box.style.height = `${room ? Math.min(h, Math.max(field + RIM, room)) : h}px`;
  }, [typing, phone, phase, q, rows.length, goH, field, room]);

  useEffect(() => {
    onLift?.(typing);
  }, [typing, onLift]);

  const open = useCallback(() => {
    if (phase === 'scan') return;
    setTyping((was) => {
      if (!was) onTyping(true);
      return true;
    });
    setActive(-1);
  }, [phase, onTyping]);
  const close = useCallback(
    (silent = false) => {
      setTyping((was) => {
        if (was && !silent) onTyping(false);
        return false;
      });
      setText('');
      setActive(-1);
      inputRef.current?.blur();
    },
    [onTyping],
  );
  // the page closes it on Back (onTyping is not called again then)
  useEffect(() => {
    const off = () => close(true);
    window.addEventListener('rn:ask-close', off);
    return () => window.removeEventListener('rn:ask-close', off);
  }, [close]);
  useEffect(() => {
    if (!focusKey) return undefined;
    const id = requestAnimationFrame(() => {
      open();
      inputRef.current?.focus({ preventScroll: true });
    });
    return () => cancelAnimationFrame(id);
  }, [focusKey, open]);
  // a press outside the box closes it (the veil is outside it)
  useEffect(() => {
    if (!typing) return undefined;
    const off = (e: PointerEvent) => {
      if (!boxRef.current?.contains(e.target as Node)) close();
    };
    window.addEventListener('pointerdown', off, true);
    return () => window.removeEventListener('pointerdown', off, true);
  }, [typing, close]);

  // leaving the field for something it does folds the box away silently: the page turns it into what was asked for
  const fold = () => {
    setTyping(false);
    setText('');
    setActive(-1);
    inputRef.current?.blur();
  };
  const send = (t: string) => {
    const s = t.trim();
    if (!s) return;
    fold();
    onSend(s);
  };
  const openMatch = (key: string) => {
    fold();
    onOpen(key);
  };
  const take = (lead: AskLead | null) => {
    if (lead) onTake(lead);
  };
  // the rows the arrows walk: the matches, then the search (unless it would be turned away)
  const count = rows.length + (q && !refused ? 1 : 0);
  const onKey = (e: KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      if (active >= 0 && active < rows.length) openMatch(rows[active].key);
      else if (q && !refused) send(text);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      if (count) setActive((i) => (i + 1) % count);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      if (count) setActive((i) => (i <= 0 ? count - 1 : i - 1));
    } else if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      if (text) setText('');
      else close();
    }
  };

  const busy = phase === 'scan';
  const answered = phase === 'answer' && !typing;
  const showQuestion = busy || answered;
  const disc = busy ? 'stop' : answered ? 'clear' : 'take';
  const eyebrow = busy ? 'Scanning his feed' : answered ? answerNote : handle && phone ? `Ask @${handle}` : `Search his last ${scope} posts`;
  const lead = roll.cur;

  return (
    <div className={cn('rn-askwrap', phone && 'is-phone', phone && phase === 'rest' && 'is-hero')}>
      <div
        ref={boxRef}
        className={cn('rn-ask', typing && 'is-typing', busy && 'is-busy', answered && 'is-answered')}
        onPointerDown={(e) => {
          if ((e.target as HTMLElement).closest('button')) return;
          if (busy) return;
          e.preventDefault();
          open();
          inputRef.current?.focus({ preventScroll: true });
        }}
      >
        {/* depth, one layer per shadow so only opacity ever moves: contact, ambient and a faint crimson at rest; the
            deeper shadow and the stronger crimson come up when the box comes forward */}
        <span className="rn-ask__sh is-amb" aria-hidden="true" />
        <span className="rn-ask__cast" aria-hidden="true">
          <i className="rn-ask__castl" />
        </span>
        <span className="rn-ask__sh is-deep" aria-hidden="true" />
        <span className="rn-ask__sh is-heat" aria-hidden="true" />
        <span className="rn-ask__sh is-contact" aria-hidden="true" />
        <Streak radius={phone ? 28 : 34} />
        <span className="rn-ask__edge" aria-hidden="true" />

        {/* at rest, scanning or answered: the lead (or the question), what a tap does, and the disc */}
        <div className="rn-ask__head" aria-hidden={typing}>
          <div className="rn-ask__in">
            <p className="rn-ask__k">
              {busy ? <span className="rn-ask__live" aria-live="polite">{`Reading 0 of ${scope}`}</span> : <span>{eyebrow}</span>}
            </p>
            <div className="rn-ask__line">
              {showQuestion ? (
                <span className="rn-ask__q">{question}</span>
              ) : (
                <>
                  <span className="rn-ask__roll">
                    {lead ? (
                      <TypedLine text={lead.text} still={still} onWhole={setWhole} />
                    ) : (
                      <span className="rn-ask__ri is-invite">Ask about anyone or anything in his posts</span>
                    )}
                  </span>
                </>
              )}
            </div>
            {!showQuestion && lead && whole ? (
              <p key={`n${roll.k}`} className="rn-ask__note is-in">
                <span>{lead.sub}</span>
              </p>
            ) : (
              <p className="rn-ask__note" aria-hidden="true" />
            )}
          </div>
          <div className="rn-ask__side">
            <button
              type="button"
              className={cn('rn-ask__disc', `is-${disc}`, disc === 'take' && !lead && 'is-idle')}
              tabIndex={typing ? -1 : 0}
              aria-disabled={disc === 'take' && !lead}
              onPointerDown={(e) => e.preventDefault()}
              onClick={() => (disc === 'stop' ? onStop() : disc === 'clear' ? onClear() : take(lead))}
              aria-label={disc === 'stop' ? 'Stop the scan' : disc === 'clear' ? 'Clear the search' : lead ? `${lead.search ? 'Search' : 'Open'}: ${lead.text}` : 'Type a search first'}
            >
              {disc === 'take' ? lead?.search ? <ArrowUp strokeWidth={2.6} aria-hidden="true" /> : <ArrowRight strokeWidth={2.6} aria-hidden="true" /> : disc === 'clear' ? <X strokeWidth={2.6} aria-hidden="true" /> : <i aria-hidden="true" />}
            </button>
            <span className={cn('rn-ask__cost', (disc !== 'take' || !lead) && 'is-off')}>
              {lead?.search ? (
                <>
                  <b>{cost}</b> bites
                </>
              ) : (
                <b>Opens, free</b>
              )}
            </span>
          </div>
          {busy ? (
            <div className="rn-ask__got" aria-hidden="true">
              {caught.map((c) => (
                <span key={c.id} className="rn-ask__slot" data-caught={c.id}>
                  {/* eslint-disable-next-line @next/next/no-img-element */}
                  <img src={c.cover} alt="" draggable={false} />
                </span>
              ))}
            </div>
          ) : null}
        </div>

        {/* typing: the compact field row, then what matches and the search */}
        <div className="rn-ask__field">
          {!text ? <i className="rn-ask__caret" aria-hidden="true" /> : null}
          {!text ? (
            <span className="rn-ask__ph" aria-hidden="true">
              Search his last {scope} posts
            </span>
          ) : null}
          <input
            ref={inputRef}
            className={cn('rn-ask__input', !text && 'is-empty')}
            value={text}
            onChange={(e) => {
              setText(e.target.value);
              setActive(-1);
            }}
            onFocus={open}
            onBlur={() => close()}
            onKeyDown={onKey}
            aria-label={`Search his last ${scope} posts`}
            aria-autocomplete="list"
            aria-controls="rn-askl"
            autoComplete="off"
            autoCorrect="off"
            autoCapitalize="none"
            spellCheck={false}
            enterKeyHint="search"
          />
          {text ? (
            <button type="button" className="rn-ask__wipe" onPointerDown={(e) => e.preventDefault()} onClick={() => setText('')} aria-label="Clear what is typed">
              <X strokeWidth={2.4} aria-hidden="true" />
            </button>
          ) : null}
        </div>
        <div id="rn-askl" className="rn-askl" role="listbox" aria-label="Matches" aria-hidden={!typing}>
          {!q ? (
            <p className="rn-askl__hint">A person, a brand, a place or an idea</p>
          ) : (
            <>
              {rows.map((m, i) => (
                <button
                  key={m.key}
                  type="button"
                  role="option"
                  aria-selected={i === active}
                  className={cn('rn-askl__m', i === active && 'is-active')}
                  style={{ ['--i' as string]: i }}
                  onPointerDown={(e) => e.preventDefault()}
                  onClick={() => openMatch(m.key)}
                >
                  <span className="rn-askl__pic" aria-hidden="true">
                    {m.cover ? <img src={m.cover} alt="" loading="lazy" decoding="async" draggable={false} /> : null}
                  </span>
                  <span className="rn-askl__t">
                    <b>{m.name}</b>
                    <small>
                      {m.kind} · {m.posts} {m.posts === 1 ? 'post' : 'posts'}, {m.landed === 0 ? 'none' : m.landed} landed
                    </small>
                  </span>
                  <span className="rn-askl__free">Free</span>
                </button>
              ))}
              {refused ? (
                <div className="rn-askl__go is-no" style={{ ['--i' as string]: rows.length }}>
                  <span className="rn-askl__t">
                    <b>Can&rsquo;t search for &ldquo;{text.trim()}&rdquo;</b>
                    <small>{refused.reason ?? 'The reader cannot answer this from his posts.'}</small>
                  </span>
                </div>
              ) : (
                <button
                  type="button"
                  role="option"
                  aria-selected={active === rows.length}
                  className={cn('rn-askl__go', active === rows.length && 'is-active')}
                  style={{ ['--i' as string]: rows.length }}
                  onPointerDown={(e) => e.preventDefault()}
                  onClick={() => send(text)}
                >
                  <span className="rn-askl__t">
                    <small>Search his last {scope} posts for</small>
                    <b>&ldquo;{text.trim()}&rdquo;</b>
                  </span>
                  <span className="rn-askl__cost">
                    <b>{cost}</b> bites
                  </span>
                </button>
              )}
            </>
          )}
        </div>
      </div>
    </div>
  );
}

export default memo(Ask);
