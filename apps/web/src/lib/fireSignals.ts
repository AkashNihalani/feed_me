export type FireSignalContext = 'own' | 'cross' | 'anchor';

export type FireSignalDefinition = {
  code: string;
  context: FireSignalContext;
  shortLabel: string;
  headline: string;
};

export const FIRE_SIGNAL_DEFINITIONS: FireSignalDefinition[] = [
  { code: 'TRACKING_BASE', context: 'own', shortLabel: 'Tracking', headline: '' },
  { code: 'OWN_PATTERN', context: 'own', shortLabel: 'Own Pattern', headline: 'Pattern repeating for you' },
  { code: 'CROSS_PATTERN', context: 'cross', shortLabel: 'Cross Pattern', headline: 'Pattern spreading across the feed' },
  { code: 'ANCHOR_PATTERN', context: 'anchor', shortLabel: 'Anchor Pattern', headline: 'Pattern beating the anchor' },
];

const MECHANIC_LABELS: Record<string, string> = {
  REVEAL: 'Transformation reveal',
  PROCESS: 'How-it-works walkthrough',
  REACTION: 'Reaction-led clip',
  SHOWCASE: 'Highlight showcase',
  STORY: 'Story-led post',
  COMPARE: 'Comparison post',
  LIST: 'List-style post',
  CHALLENGE: 'Challenge-led post',
  CONVERSE: 'Q&A / talking-head post',
  ACCESS: 'Behind-the-scenes access',
  ANNOUNCE: 'Launch / news post',
  COLLAB: 'Collaboration post',
  SOCIAL_PROOF: 'Proof / testimonial post',
  AESTHETIC: 'Aesthetic post',
  EDUCATE: 'Explainer post',
};

const CUE_LABELS: Record<string, Record<string, string>> = {
  opening_move: {
    RESULT_FIRST: 'Result shown first',
    PERSON_FIRST: 'Person on screen first',
    TEXT_FIRST: 'Key point in first frame',
    OBJECT_FIRST: 'Product shown first',
    ACTION_FIRST: 'Action starts immediately',
    SCENE_FIRST: 'Scene set first',
  },
  proof_mode: {
    LIVE_DEMO: 'Live demo',
    VISUAL_RESULT: 'Visible payoff',
    EXPERT_TALK: 'Expert-led explanation',
    SOCIAL_PROOF: 'Customer/result proof',
    DATA_PROOF: 'Stats on screen',
    ACCESS_PROOF: 'Behind-the-scenes proof',
    PROOF_NONE: 'No explicit proof cue',
  },
  pacing: {
    PACING_SLOW: 'Slow build',
    PACING_MEDIUM: 'Steady pacing',
    PACING_FAST: 'Quick-cut edit',
  },
  audio_mode: {
    AUDIO_DIRECT_SPEECH: 'Talking to camera',
    AUDIO_VOICEOVER: 'Voiceover',
    AUDIO_SOURCE_LIVE: 'Natural live sound',
    AUDIO_MUSIC_LED: 'Music-led edit',
    AUDIO_ASMR: 'ASMR-style sound',
    AUDIO_MINIMAL: 'Minimal audio',
  },
  style: {
    STYLE_UGC: 'UGC feel',
    STYLE_STUDIO: 'Studio-produced',
    STYLE_TEXT_DRIVEN: 'Text-led format',
    STYLE_MONTAGE: 'Montage edit',
    STYLE_CINEMATIC: 'Cinematic edit',
    STYLE_SCREEN_RECORD: 'Screen demo',
  },
  face: {
    FACE_SINGLE: 'One person on screen',
    FACE_NONE: 'No person on screen',
    FACE_MULTIPLE: 'Multiple people on screen',
  },
  density: {
    DENSITY_MINIMAL: 'Clean visual',
    DENSITY_MEDIUM: 'Balanced visual load',
    DENSITY_BUSY: 'Dense visual',
  },
  text_overlay: {
    TEXT_NONE: 'No on-screen text',
    TEXT_LIGHT: 'Light text support',
    TEXT_HEAVY: 'Text-led frames',
  },
  duration_bucket: {
    DUR_SHORT: 'Under 15s',
    DUR_MEDIUM: '15-30s length',
    DUR_LONG: '30-60s length',
    DUR_EXTENDED: '60s+ length',
  },
  depth: {
    DEPTH_SINGLE: 'Single frame',
    DEPTH_MINI: 'Mini carousel',
    DEPTH_STANDARD: 'Standard carousel',
    DEPTH_DEEP: 'Deep carousel',
  },
};

export const FIRE_SIGNAL_META = Object.fromEntries(
  FIRE_SIGNAL_DEFINITIONS.map((definition) => [definition.code, definition]),
) as Record<string, FireSignalDefinition>;

export function getFireSignalMeta(code: string | null | undefined): FireSignalDefinition | null {
  if (!code) return null;
  return FIRE_SIGNAL_META[code] ?? null;
}

export function getPatternMechanicLabel(value: string | null | undefined): string | null {
  if (!value) return null;
  return MECHANIC_LABELS[value] ?? null;
}

export function getPatternCueLabel(
  key: string | null | undefined,
  value: string | null | undefined,
): string | null {
  if (!key || !value) return null;
  return CUE_LABELS[key]?.[value] ?? null;
}
