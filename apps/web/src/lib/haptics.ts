'use client';

import { WebHaptics } from 'web-haptics';

type HapticIntent = 'navSwitch' | 'navReselect' | 'snapLock';
type TelegramHaptics = { impactOccurred: (style: 'light' | 'medium') => void };
type CapacitorHaptics = { impact: (options: { style: string }) => Promise<void> | void };
type WebkitHapticHandler = { postMessage: (payload: { style: 'light' | 'medium' }) => void };
type AppNavigator = Navigator & {
  Capacitor?: { Plugins?: { Haptics?: CapacitorHaptics } };
  Telegram?: { WebApp?: { HapticFeedback?: TelegramHaptics } };
  webkit?: { messageHandlers?: { haptic?: WebkitHapticHandler } };
};

const HAPTIC_PATTERNS: Record<HapticIntent, number> = {
  navSwitch: 26,
  navReselect: 22,
  snapLock: 8,
};

const webHaptics = typeof window !== 'undefined' ? new WebHaptics({ showSwitch: false, debug: false }) : null;

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

  const play = (intent: HapticIntent) => {
    if (!hasWindow) return;
    const pattern = HAPTIC_PATTERNS[intent] ?? 8;
    const impactStyle = intent === 'snapLock' ? 'light' : 'medium';
    const webPreset = intent === 'snapLock' ? 'selection' : 'medium';

    try {
      const capacitorHaptics = appNavigator?.Capacitor?.Plugins?.Haptics;
      if (capacitorHaptics?.impact) {
        void capacitorHaptics.impact({ style: impactStyle.toUpperCase() });
        return;
      }
    } catch {}

    try {
      const tg = appNavigator?.Telegram?.WebApp?.HapticFeedback;
      if (tg?.impactOccurred) {
        tg.impactOccurred(impactStyle);
        return;
      }
    } catch {}

    try {
      const webkitHaptic = appNavigator?.webkit?.messageHandlers?.haptic;
      if (webkitHaptic?.postMessage) {
        webkitHaptic.postMessage({ style: impactStyle });
        return;
      }
    } catch {}

    try {
      if (webHaptics) {
        void webHaptics.trigger(webPreset);
        return;
      }
    } catch {}

    try {
      if (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') {
        navigator.vibrate(pattern);
      }
    } catch {}
  };

  return {
    play,
    isSupported,
  };
}
