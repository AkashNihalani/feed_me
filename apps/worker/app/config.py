import json
import os

def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value or ""


def _parse_env_list(value: str) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass
    if "|" in raw:
        return [x.strip() for x in raw.split("|") if x.strip()]
    return [x.strip() for x in raw.split(",") if x.strip()]


def _align_schema(header: list[str], descriptions: list[str]) -> tuple[list[str], list[str]]:
    if len(descriptions) == len(header):
        return header, descriptions
    if len(descriptions) < len(header):
        descriptions = descriptions + [""] * (len(header) - len(descriptions))
        return header, descriptions
    return header, descriptions[: len(header)]

POSTGRES_DSN = _get_env("POSTGRES_DSN", required=True)
SPREADSHEET_ID = _get_env("SPREADSHEET_ID", required=True)

APIFY_TOKEN = _get_env("APIFY_TOKEN", required=True)
APIFY_ACTOR_ID = _get_env("APIFY_ACTOR_ID", required=True)

APIFY_MAX_ITEMS = int(_get_env("APIFY_MAX_ITEMS", "100"))
APIFY_RUN_TIMEOUT_SECONDS = int(_get_env("APIFY_RUN_TIMEOUT_SECONDS", "900"))
APIFY_POLL_INTERVAL_SECONDS = int(_get_env("APIFY_POLL_INTERVAL_SECONDS", "10"))

APIFY_INPUT_TEMPLATE_DAILY = _get_env(
    "APIFY_INPUT_TEMPLATE_DAILY",
    _get_env(
        "APIFY_INPUT_TEMPLATE",
        '{"addParentData":false,"directUrls":["https://www.instagram.com/{handle}/"],"onlyPostsNewerThan":"3 days","resultsLimit":100,"resultsType":"posts","searchType":"user"}',
    ),
)

APIFY_INPUT_TEMPLATE_WEEKLY = _get_env(
    "APIFY_INPUT_TEMPLATE_WEEKLY",
    APIFY_INPUT_TEMPLATE_DAILY,
)

APIFY_INPUT_TEMPLATE_DETAILS = _get_env(
    "APIFY_INPUT_TEMPLATE_DETAILS",
    '{"addParentData":false,"directUrls":["https://www.instagram.com/{handle}/"],"resultsLimit":1,"resultsType":"details","searchType":"user"}',
)

APIFY_INPUT_TEMPLATE_POST_URL = _get_env(
    "APIFY_INPUT_TEMPLATE_POST_URL",
    '{"addParentData":false,"directUrls":["{post_url}"],"resultsLimit":1,"resultsType":"posts"}',
)

SHEET_HEADER = _get_env(
    "SHEET_HEADER",
    "post_url|posted_at|handle|display_name|media_type|is_pinned|views|likes|comments|perf_score|velocity|velocity_percentile|velocity_stage|caption|hashtags|caption_mentions|tagged_users|music_info|paid_partnership|sponsors|display_url|video_url|scanned_at|last_updated_at",
)

SHEET_HEADER_LIST = _parse_env_list(SHEET_HEADER)

SHEET_DESCRIPTIONS = _get_env(
    "SHEET_DESCRIPTIONS",
    "Unique link to post (do not edit)|Post date/time from Instagram (DD-MM-YY hh:mm AM/PM)|Instagram handle|Display name|Format: Video / Image / Sidecar (carousel)|Whether pinned by creator|Total views (Reels)|Total likes|Total comments|Engagement rate percent (backend computed: video by views, image/sidecar by weekly followers baseline)|Velocity emoji from percentile bands (rocket/fire/check/sleeping; clover for late bloomer)|Velocity percentile rank at same checkpoint cohort using metric_per_day (1% = top performer)|Velocity stage (D1 post added, D2 next-day update, D3 checkpoint, D7 gate, D21 final)|Post caption text|Hashtags comma separated|Mentions found in caption|Users tagged in post|Music used short|Whether post is a paid partnership|Brands involved or sponsors|Thumbnail preview link|Video file link (Reels)|When system scanned this post|When this row was last updated",
)

SHEET_DESCRIPTION_LIST = _parse_env_list(SHEET_DESCRIPTIONS)
SHEET_HEADER_LIST, SHEET_DESCRIPTION_LIST = _align_schema(SHEET_HEADER_LIST, SHEET_DESCRIPTION_LIST)

RETRY_BACKOFF_MINUTES = [
    int(x.strip()) for x in _get_env("RETRY_BACKOFF_MINUTES", "15,15,15,15,15,15").split(",") if x.strip()
] or [15]
APIFY_COOLDOWN_TRIGGER_FAILURES = int(_get_env("APIFY_COOLDOWN_TRIGGER_FAILURES", "5"))
APIFY_COOLDOWN_HOURS = int(_get_env("APIFY_COOLDOWN_HOURS", "3"))

IGNORE_SHEETS = [
    s.strip()
    for s in _get_env("IGNORE_SHEETS", "Config,Logs,README").split(",")
    if s.strip()
]

# Billing defaults (per subscriber per month)
INCLUDED_UPDATES_PER_MONTH = int(_get_env("INCLUDED_UPDATES_PER_MONTH", "1000"))
OVERAGE_RATE_PER_UPDATE = float(_get_env("OVERAGE_RATE_PER_UPDATE", "2.5"))

# Embeddings
EMBEDDING_API_KEY = _get_env(
    "EMBEDDING_API_KEY",
    _get_env("OPENROUTER_API_KEY", _get_env("OPENAI_API_KEY", "")),
)
EMBEDDING_BASE_URL = _get_env(
    "EMBEDDING_BASE_URL",
    "https://openrouter.ai/api/v1" if _get_env("OPENROUTER_API_KEY", "") else "https://api.openai.com/v1",
)
OPENROUTER_SITE_URL = _get_env("OPENROUTER_SITE_URL", "")
OPENROUTER_APP_NAME = _get_env("OPENROUTER_APP_NAME", "feedme-worker")
OPENAI_EMBED_MODEL = _get_env("OPENAI_EMBED_MODEL", _get_env("EMBEDDING_MODEL", "text-embedding-3-small"))
EMBED_ONLY_TAGS = _parse_env_list(_get_env("EMBED_ONLY_TAGS", "🔥,🚀"))
EMBED_BATCH_LIMIT = int(_get_env("EMBED_BATCH_LIMIT", "100"))
EMBED_SIGNAL_TYPES = _parse_env_list(_get_env("EMBED_SIGNAL_TYPES", "caption_semantic,performance_semantic"))
