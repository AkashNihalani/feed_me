export const EXPORT_FIELD_GROUPS = [
  {
    id: 'identity',
    label: 'Identity',
    fields: [
      { id: 'handle', label: 'Handle' },
      { id: 'published_at_ist', label: 'Published At (IST)' },
      { id: 'media_type', label: 'Media Type' },
      { id: 'post_link', label: 'Instagram Post Link' },
    ],
  },
  {
    id: 'content',
    label: 'Content',
    fields: [
      { id: 'caption', label: 'Caption' },
      { id: 'thumbnail_link', label: 'Thumbnail Link' },
    ],
  },
  {
    id: 'performance',
    label: 'Latest Performance',
    fields: [
      { id: 'views', label: 'Latest Views' },
      { id: 'likes', label: 'Latest Likes' },
      { id: 'comments', label: 'Latest Comments' },
    ],
  },
  {
    id: 'ranking',
    label: 'Latest Ranking',
    fields: [
      { id: 'overall_percentile', label: 'Latest Overall Percentile' },
      { id: 'views_percentile', label: 'Latest Views Percentile' },
      { id: 'likes_percentile', label: 'Latest Likes Percentile' },
      { id: 'comments_percentile', label: 'Latest Comments Percentile' },
      { id: 'delta_from_d1', label: 'Latest Delta From D1' },
    ],
  },
  {
    id: 'd1_tracking',
    label: 'D1 Tracking',
    fields: [
      { id: 'd1_views', label: 'D1 Views' },
      { id: 'd1_likes', label: 'D1 Likes' },
      { id: 'd1_comments', label: 'D1 Comments' },
      { id: 'd1_overall_percentile', label: 'D1 Overall Percentile' },
      { id: 'd1_views_percentile', label: 'D1 Views Percentile' },
      { id: 'd1_likes_percentile', label: 'D1 Likes Percentile' },
      { id: 'd1_comments_percentile', label: 'D1 Comments Percentile' },
    ],
  },
  {
    id: 'd3_tracking',
    label: 'D3 Tracking',
    fields: [
      { id: 'd3_views', label: 'D3 Views' },
      { id: 'd3_likes', label: 'D3 Likes' },
      { id: 'd3_comments', label: 'D3 Comments' },
      { id: 'd3_overall_percentile', label: 'D3 Overall Percentile' },
      { id: 'd3_views_percentile', label: 'D3 Views Percentile' },
      { id: 'd3_likes_percentile', label: 'D3 Likes Percentile' },
      { id: 'd3_comments_percentile', label: 'D3 Comments Percentile' },
      { id: 'd3_delta_from_d1', label: 'D3 Delta From D1' },
    ],
  },
  {
    id: 'd7_tracking',
    label: 'D7 Tracking',
    fields: [
      { id: 'd7_views', label: 'D7 Views' },
      { id: 'd7_likes', label: 'D7 Likes' },
      { id: 'd7_comments', label: 'D7 Comments' },
      { id: 'd7_overall_percentile', label: 'D7 Overall Percentile' },
      { id: 'd7_views_percentile', label: 'D7 Views Percentile' },
      { id: 'd7_likes_percentile', label: 'D7 Likes Percentile' },
      { id: 'd7_comments_percentile', label: 'D7 Comments Percentile' },
      { id: 'd7_delta_from_d1', label: 'D7 Delta From D1' },
    ],
  },
  {
    id: 'd21_tracking',
    label: 'D21 Tracking',
    fields: [
      { id: 'd21_views', label: 'D21 Views' },
      { id: 'd21_likes', label: 'D21 Likes' },
      { id: 'd21_comments', label: 'D21 Comments' },
      { id: 'd21_overall_percentile', label: 'D21 Overall Percentile' },
      { id: 'd21_views_percentile', label: 'D21 Views Percentile' },
      { id: 'd21_likes_percentile', label: 'D21 Likes Percentile' },
      { id: 'd21_comments_percentile', label: 'D21 Comments Percentile' },
      { id: 'd21_delta_from_d1', label: 'D21 Delta From D1' },
    ],
  },
] as const;

export type ExportFieldGroupId = (typeof EXPORT_FIELD_GROUPS)[number]['id'];
export type ExportFieldId = (typeof EXPORT_FIELD_GROUPS)[number]['fields'][number]['id'];

export const ALL_EXPORT_FIELD_IDS = EXPORT_FIELD_GROUPS.flatMap((group) => (
  group.fields.map((field) => field.id)
)) as ExportFieldId[];

export const DEFAULT_EXPORT_FIELD_IDS = ALL_EXPORT_FIELD_IDS;

export const MINIMAL_EXPORT_FIELD_IDS = [
  'handle',
  'published_at_ist',
  'media_type',
  'post_link',
  'views',
  'likes',
  'comments',
  'overall_percentile',
] as const satisfies readonly ExportFieldId[];

export const EXPORT_FIELD_LABELS = Object.fromEntries(
  EXPORT_FIELD_GROUPS.flatMap((group) => group.fields.map((field) => [field.id, field.label])),
) as Record<ExportFieldId, string>;

export const EXPORT_FIELD_ID_SET = new Set<ExportFieldId>(ALL_EXPORT_FIELD_IDS);
