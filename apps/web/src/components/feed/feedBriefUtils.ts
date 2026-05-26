export type FeedBrief = {
  relationship: string;
  accountTypes: string[];
  category: string;
  geography: string;
  audience: string;
  priorities: string[];
  note: string;
};

export const RELATIONSHIP_OPTIONS = [
  { label: 'Watching competitors', description: 'Head-to-head with brands or creators in our lane.' },
  { label: 'Tracking inspiration', description: 'Accounts I want to learn craft from.' },
  { label: 'Monitoring our own brand', description: 'Me, my team, or my client.' },
  { label: 'Category research', description: 'Broad awareness across a space.' },
];
export const ACCOUNT_TYPE_OPTIONS = ['Creators', 'Brands', 'Publications', 'Local businesses', 'Athletes / Sports'];
export const CATEGORY_OPTIONS = ['Beauty', 'Food', 'Fashion', 'Fitness', 'Tech', 'Music', 'Sports'];
export const GEOGRAPHY_OPTIONS = ['Global', 'India', 'Mumbai'];
export const PRIORITY_OPTIONS = [
  'Breakouts & early heat',
  'Long-haul performers',
  'Comment-led posts',
  'Audience movement',
  'Format shifts',
  'Fading content',
  'Competitor gaps',
];

export const CREATE_FEED_STEPS = [
  'Why',
  'Who',
  'Audience',
  'First',
  'Context',
  'Brief',
] as const;

export const CREATE_FEED_LAST_STEP = CREATE_FEED_STEPS.length - 1;
export const MAX_PRIORITY_SELECTIONS = 3;

export function defaultFeedBrief(): FeedBrief {
  return {
    relationship: RELATIONSHIP_OPTIONS[0]!.label,
    accountTypes: ['Creators'],
    category: '',
    geography: 'Global',
    audience: '',
    priorities: ['Breakouts & early heat', 'Long-haul performers', 'Comment-led posts'],
    note: '',
  };
}

function formatList(items: string[], fallback = 'metric-moving signals') {
  const clean = items.map((item) => item.trim()).filter(Boolean);
  if (clean.length === 0) return fallback;
  if (clean.length === 1) return clean[0];
  if (clean.length === 2) return `${clean[0]} and ${clean[1]}`;
  return `${clean.slice(0, -1).join(', ')}, and ${clean[clean.length - 1]}`;
}

function relationshipPhrase(value: string) {
  switch (value) {
    case 'Watching competitors':
      return 'competitive awareness';
    case 'Tracking inspiration':
      return 'craft inspiration';
    case 'Monitoring our own brand':
      return 'own-brand monitoring';
    case 'Category research':
      return 'category research';
    default:
      return value ? value.toLowerCase() : 'social tracking';
  }
}

export function buildFeedBriefText(brief: FeedBrief, fallbackTitle: string) {
  const relationship = brief.relationship.trim();
  const accountTypes = brief.accountTypes.map((value) => value.trim()).filter(Boolean);
  const category = brief.category.trim();
  const geography = brief.geography.trim();
  const audience = brief.audience.trim();
  const priorities = brief.priorities.map((value) => value.trim()).filter(Boolean);
  const note = brief.note.trim();
  const categoryPhrase = category || fallbackTitle || 'social activity';
  const accountPhrase = formatList(accountTypes, 'accounts');
  const scopePhrase = geography
    ? geography.toLowerCase() === 'global'
      ? 'globally'
      : `in ${geography}`
    : 'wherever the audience lives';
  const parts = [
    `This feed tracks ${categoryPhrase} ${accountPhrase} ${scopePhrase} for ${relationshipPhrase(relationship)}.`,
    audience ? `The audience that matters: ${audience}.` : '',
    `Brief will surface ${formatList(priorities)} first, while still flagging follower movement, fades, and gaps when they're real.`,
    note ? `Local context: ${note}` : '',
  ].filter(Boolean);
  return parts.join(' ').trim() || `This feed tracks ${fallbackTitle}. Brief should explain metric-moving account activity.`;
}

export function normalizeFeedBrief(value: unknown): FeedBrief {
  const row = value && typeof value === 'object' && !Array.isArray(value)
    ? value as Record<string, unknown>
    : {};
  const location = row.location && typeof row.location === 'object' && !Array.isArray(row.location)
    ? row.location as Record<string, unknown>
    : null;
  const locationText = typeof row.location === 'string' ? row.location : location?.value;
  const legacyOutcomes = Array.isArray(row.outcomes)
    ? row.outcomes
    : Array.isArray(row.outcomesPriority)
      ? row.outcomesPriority
      : Array.isArray(row.priorities)
        ? row.priorities
        : [];
  const accountTypes = Array.isArray(row.accountTypes)
    ? row.accountTypes.map((item) => String(item || '').trim()).filter(Boolean)
    : String(row.accountType || row.account_type || '').trim()
      ? [String(row.accountType || row.account_type || '').trim()]
      : [];
  const priorities = legacyOutcomes.map((item) => String(item || '').trim()).filter(Boolean).slice(0, MAX_PRIORITY_SELECTIONS);
  const legacyMarket = String(row.market || locationText || '').trim();
  return {
    relationship: String(row.relationship || row.trackingMode || row.trackingType || row.tracking_type || '').trim() || RELATIONSHIP_OPTIONS[0]!.label,
    accountTypes: accountTypes.length > 0 ? accountTypes : defaultFeedBrief().accountTypes,
    category: String(row.category || legacyMarket || '').trim(),
    geography: String(row.geography || '').trim() || (legacyMarket ? '' : defaultFeedBrief().geography),
    audience: String(row.audience || '').trim(),
    priorities: priorities.length > 0 ? priorities : defaultFeedBrief().priorities,
    note: String(row.note || row.freeNote || row.free_note || '').trim(),
  };
}
