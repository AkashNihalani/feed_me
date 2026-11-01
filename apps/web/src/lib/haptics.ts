'use client';

import { WebHaptics } from 'web-haptics';

/* A tap of the phone's haptics where the app commits to something: a tab switched to, a pick made, a pull that locks
   open, the finger landing on a circle while it slides. Inside a native shell (Capacitor, a WebView's `haptic`
   handler, Telegram) they're the system's own; in Android browsers, a short vibration. iOS Safari has no web haptics:
   web-haptics' hidden-switch path only ever played on iOS 17.4 to 26.4, and is silent from 26.5 on */
type HapticIntent = 'navSwitch' | 'navReselect' | 'snapLock' | 'pick';
type TelegramHaptics = { impactOccurred: (style: 'light' | 'medium') => void };
type CapacitorHaptics = { impact: (options: { style: string }) => Promise<void> | void };
type WebkitHapticHandler = { postMessage: (payload: { style: 'light' | 'medium' }) => void };
type AppNavigator = Navigator & {
  Capacitor?: { Plugins?: { Haptics?: CapacitorHaptics } };
  Telegram?: { WebApp?: { HapticFeedback?: TelegramHaptics } };
  webkit?: { messageHandlers?: { haptic?: WebkitHapticHandler } };
};

// each intent: a vibration (ms) where that's all there is, the system's impact style, and web-haptics' preset
const HAPTICS: Record<HapticIntent, { pattern: number; impact: 'light' | 'medium'; preset: string }> = {
  navSwitch: { pattern: 26, impact: 'medium', preset: 'medium' },
  navReselect: { pattern: 22, impact: 'medium', preset: 'medium' },
  snapLock: { pattern: 8, impact: 'light', preset: 'selection' },
  pick: { pattern: 14, impact: 'light', preset: 'light' },
};

const webHaptics = typeof window !== 'undefined' ? new WebHaptics({ showSwitch: false, debug: false }) : null;

// (one function for the app's life: safe to call from handlers and callbacks that never re-render)
export function playHaptic(intent: HapticIntent) {
  if (typeof window === 'undefined') return;
  const appNavigator = window.navigator as AppNavigator;
  const { pattern, impact, preset } = HAPTICS[intent];

  try {
    const capacitorHaptics = appNavigator.Capacitor?.Plugins?.Haptics;
    if (capacitorHaptics?.impact) {
      void capacitorHaptics.impact({ style: impact.toUpperCase() });
      return;
    }
  } catch {}

  try {
    const tg = appNavigator.Telegram?.WebApp?.HapticFeedback;
    if (tg?.impactOccurred) {
      tg.impactOccurred(impact);
      return;
    }
  } catch {}

  try {
    const webkitHaptic = appNavigator.webkit?.messageHandlers?.haptic;
    if (webkitHaptic?.postMessage) {
      webkitHaptic.postMessage({ style: impact });
      return;
    }
  } catch {}

  try {
    if (webHaptics) {
      void webHaptics.trigger(preset);
      return;
    }
  } catch {}

  try {
    if (typeof navigator.vibrate === 'function') navigator.vibrate(pattern);
  } catch {}
}

export function useAppHaptics() {
  const hasWindow = typeof window !== 'undefined';
  const appNavigator = hasWindow ? (window.navigator as AppNavigator) : null;
  const isSupported =
    hasWindow &&
    (
      (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') ||
      !!appNavigator?.Telegram?.WebApp?.HapticFeedback ||
      !!appNavigator?.webkit?.messageHandlers?.haptic ||
      !!appNavigator?.Capacitor?.Plugins?.Haptics
    );

  return {
    play: playHaptic,
    isSupported,
  };
}
