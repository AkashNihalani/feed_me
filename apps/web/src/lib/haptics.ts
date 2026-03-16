'use client';

import { WebHaptics } from 'web-haptics';

type HapticIntent = 'navSwitch' | 'navReselect' | 'snapLock';

const HAPTIC_PATTERNS: Record<HapticIntent, number> = {
  navSwitch: 26,
  navReselect: 22,
  snapLock: 8,
};

const webHaptics = typeof window !== 'undefined' ? new WebHaptics({ showSwitch: false, debug: false }) : null;

export function useAppHaptics() {
  const hasWindow = typeof window !== 'undefined';
  const isSupported =
    hasWindow &&
    (
      (typeof navigator !== 'undefined' && typeof navigator.vibrate === 'function') ||
      !!(window as any)?.Telegram?.WebApp?.HapticFeedback ||
      !!(window as any)?.webkit?.messageHandlers?.haptic ||
      !!(window as any)?.Capacitor?.Plugins?.Haptics
    );

  const play = (intent: HapticIntent) => {
    if (!hasWindow) return;
    const pattern = HAPTIC_PATTERNS[intent] ?? 8;
    const impactStyle = intent === 'snapLock' ? 'light' : 'medium';
    const webPreset = intent === 'snapLock' ? 'selection' : 'medium';

    try {
      const capacitorHaptics = (window as any)?.Capacitor?.Plugins?.Haptics;
      if (capacitorHaptics?.impact) {
        void capacitorHaptics.impact({ style: impactStyle.toUpperCase() });
        return;
      }
    } catch {}

    try {
      const tg = (window as any)?.Telegram?.WebApp?.HapticFeedback;
      if (tg?.impactOccurred) {
        tg.impactOccurred(impactStyle);
        return;
      }
    } catch {}

    try {
      const webkitHaptic = (window as any)?.webkit?.messageHandlers?.haptic;
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
