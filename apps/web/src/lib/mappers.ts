// Platform-specific mappers to transform Apify JSON into clean Excel-compatible rows

export interface ExcelRow {
  [key: string]: string | number | boolean | null;
}

// Format date to IST (Indian Standard Time) in Indian format
function formatToIST(dateString: string | undefined): string {
  if (!dateString) return '';
  
  try {
    const date = new Date(dateString);
    if (isNaN(date.getTime())) return dateString;
    
    // Format: DD/MM/YYYY, HH:MM AM/PM IST
    const options: Intl.DateTimeFormatOptions = {
      timeZone: 'Asia/Kolkata',
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
    };
    
    const formatted = new Intl.DateTimeFormat('en-IN', options).format(date);
    return `${formatted} IST`;
  } catch {
    return dateString;
  }
}

// LinkedIn Post Mapper - Custom template for apimaestro/linkedin-profile-posts
export function mapLinkedInPost(item: Record<string, unknown>): ExcelRow {
  const author = item.author as Record<string, unknown> || {};
  const stats = item.stats as Record<string, unknown> || {};
  const media = item.media as Record<string, unknown> || {};
  const postedAt = item.posted_at as Record<string, unknown> || {};

  // Build Author name
  const firstName = author.first_name as string || '';
  const lastName = author.last_name as string || '';
  const authorName = `${firstName} ${lastName}`.trim();

  // Build Media string (all image/video URLs in one cell)
  let mediaLinks = '';
  if (media.type === 'images' && Array.isArray(media.images)) {
    const images = media.images as Array<Record<string, unknown>>;
    mediaLinks = images.map((img, i) => `Image ${i + 1}: ${img.url}`).join('\n');
  } else if (media.url) {
    mediaLinks = `${media.type || 'Media'}: ${media.url}`;
  }

  // Build Reactions string (total + breakdown)
  const totalReactions = stats.total_reactions as number || 0;
  const reactionBreakdown = [
    `Total: ${totalReactions}`,
    `Like: ${stats.like || 0}`,
    `Love: ${stats.love || 0}`,
    `Celebrate: ${stats.celebrate || 0}`,
    `Support: ${stats.support || 0}`,
    `Insight: ${stats.insight || 0}`,
    `Funny: ${stats.funny || 0}`,
    `Comments: ${stats.comments || 0}`,
    `Reposts: ${stats.reposts || 0}`,
  ].join('\n');

  // Date formatting (already in readable format from actor)
  const uploadDate = postedAt.date as string || '';

  return {
    'Author': authorName,
    'Username': author.username as string || '',
    'Post URL': item.url as string || '',
    'Caption': (item.text as string || '').slice(0, 10000),
    'Media': mediaLinks,
    'Reactions': reactionBreakdown,
    'Uploaded At': uploadDate,
    'Profile Header': author.headline as string || '',
  };
}

// YouTube Video Mapper
export function mapYouTubeVideo(item: Record<string, unknown>): ExcelRow {
  return {
    'Video URL': item.url as string || `https://youtube.com/watch?v=${item.id}`,
    'Title': item.title as string || '',
    'Channel': item.channelName as string || item.channel as string || '',
    'Views': item.viewCount as number || item.views as number || 0,
    'Likes': item.likeCount as number || item.likes as number || 0,
    'Comments': item.commentCount as number || item.comments as number || 0,
    'Duration': item.duration as string || '',
    'Published (IST)': formatToIST(item.uploadDate as string || item.date as string),
    'Description': (item.description as string || '').slice(0, 2000),
  };
}

// Twitter/X Tweet Mapper
export function mapTwitterTweet(item: Record<string, unknown>): ExcelRow {
  // Handle different data structures from various Twitter scrapers
  const user = item.user as Record<string, unknown> || {};
  const author = item.author as Record<string, unknown> || {};
  
  // Try to get tweet URL first to extract username if needed
  const tweetUrl = 
    item.url as string || 
    item.tweetUrl as string ||
    '';

  // Try to get username from multiple possible locations
  let username = 
    user.username as string || 
    user.screen_name as string ||
    author.userName as string ||
    item.username as string || 
    item['user.legacy.screen_name'] as string ||
    '';

  if (!username && tweetUrl) {
    const match = tweetUrl.match(/(?:x\.com|twitter\.com)\/([^\/]+)/);
    if (match) username = match[1];
  }
  
  // Try to get display name from multiple possible locations  
  const displayName = 
    user.name as string || 
    author.name as string ||
    item.name as string ||
    item['user.legacy.name'] as string ||
    '';
  
  // Get tweet content
  const content = 
    item.full_text as string || 
    item.text as string || 
    item.tweetText as string ||
    '';
  
  // Get date
  const dateStr = 
    item.created_at as string ||
    item.createdAt as string || 
    item.date as string ||
    '';

  return {
    'Tweet URL': tweetUrl,
    'Username': username ? `@${username.replace('@', '')}` : '',
    'Display Name': displayName,
    'Content': content.slice(0, 5000),
    'Favorites': (item.favorite_count || item.favoriteCount || item.likes || item.likeCount || 0) as number,
    'Retweets': (item.retweet_count || item.retweetCount || item.retweets || 0) as number,
    'Quote Tweets': (item.quote_count || item.quoteCount || item.quotes || 0) as number,
    'Replies': (item.reply_count || item.replyCount || item.replies || 0) as number,
    'Views': (item.views_count || item.viewCount || item.views || 0) as number,
    'Date (IST)': formatToIST(dateStr),
  };
}

// Instagram Post Mapper
export function mapInstagramPost(item: Record<string, unknown>): ExcelRow {
  const owner = item.ownerUsername as string || (item.owner as Record<string, unknown>)?.username as string || '';
  
  // Date resolution (handle various Apify actors)
  const dateStr = 
    item.timestamp as string || 
    item.takenAt as string || 
    item.date as string || 
    '';

  // Duration resolution (Reels/Video)
  const duration = item.videoDuration as number || item.duration as number || 0;

  return {
    'Post URL': item.url as string || `https://instagram.com/p/${item.shortCode}`,
    'Username': owner,
    'Caption': (item.caption as string || '').slice(0, 5000),
    'Media Type': item.type as string || item.productType as string || 'post',
    'Likes': item.likesCount as number || item.likes as number || 0,
    'Comments': item.commentsCount as number || item.comments as number || 0,
    'Views': item.videoViewCount as number || item.viewCount as number || 0,
    'Duration (sec)': duration,
    'Hashtags': Array.isArray(item.hashtags) ? (item.hashtags as string[]).join(', ') : '',
    'Date (IST)': formatToIST(dateStr),
  };
}

// Master mapper dispatcher
export function mapDataToExcel(platform: string, data: Record<string, unknown>[]): ExcelRow[] {
  const mappers: Record<string, (item: Record<string, unknown>) => ExcelRow> = {
    linkedin: mapLinkedInPost,
    youtube: mapYouTubeVideo,
    x: mapTwitterTweet,
    instagram: mapInstagramPost,
  };

  const mapper = mappers[platform];
  if (!mapper) {
    throw new Error(`Unknown platform: ${platform}`);
  }

  return data.map(mapper);
}
