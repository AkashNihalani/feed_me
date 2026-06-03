from __future__ import annotations

import os


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value or ""


def _get_bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


POSTGRES_DSN = _get_env("POSTGRES_DSN", required=True)
POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS = int(
    _get_env("POSTGRES_IDLE_IN_TRANSACTION_SESSION_TIMEOUT_MS", "60000")
)
SCRAPER_PROVIDER = "brightdata"
BRIGHTDATA_API_KEY = _get_env("BRIGHTDATA_API_KEY", required=True)
BRIGHTDATA_API_BASE_URL = _get_env("BRIGHTDATA_API_BASE_URL", "https://api.brightdata.com")
BRIGHTDATA_PROFILES_DATASET_ID = _get_env("BRIGHTDATA_PROFILES_DATASET_ID", "gd_l1vikfch901nx3by4")
BRIGHTDATA_POSTS_DATASET_ID = _get_env("BRIGHTDATA_POSTS_DATASET_ID", "gd_lk5ns7kz21pck8jpis")
BRIGHTDATA_REELS_DATASET_ID = _get_env("BRIGHTDATA_REELS_DATASET_ID", "gd_lyclm20il4r5helnj")
BRIGHTDATA_POSTS_DATASET_NAME = _get_env("BRIGHTDATA_POSTS_DATASET_NAME", "Instagram - Posts")
BRIGHTDATA_DISCOVERY_OVERLAP_DAYS = int(_get_env("BRIGHTDATA_DISCOVERY_OVERLAP_DAYS", "2"))
BRIGHTDATA_DISCOVERY_MAX_POSTS = int(_get_env("BRIGHTDATA_DISCOVERY_MAX_POSTS", "100"))
BRIGHTDATA_SNAPSHOT_TIMEOUT_SECONDS = int(_get_env("BRIGHTDATA_SNAPSHOT_TIMEOUT_SECONDS", "900"))
BRIGHTDATA_POLL_INTERVAL_SECONDS = int(_get_env("BRIGHTDATA_POLL_INTERVAL_SECONDS", "10"))
BRIGHTDATA_POSTED_AT_FALLBACK_HOUR_24 = int(_get_env("BRIGHTDATA_POSTED_AT_FALLBACK_HOUR_24", "12"))
RETRY_BACKOFF_MINUTES = [
    int(x.strip()) for x in _get_env("RETRY_BACKOFF_MINUTES", "15,15,15,30,30,60").split(",") if x.strip()
] or [15]
APP_TIMEZONE = _get_env("APP_TIMEZONE", "Asia/Kolkata")
CHECKPOINT_BATCH_HOUR_24 = int(_get_env("CHECKPOINT_BATCH_HOUR_24", "23"))
CHECKPOINT_BATCH_MINUTE = int(_get_env("CHECKPOINT_BATCH_MINUTE", "30"))
CHECKPOINT_BUCKET_MINUTES = int(_get_env("CHECKPOINT_BUCKET_MINUTES", "60"))
DISCOVERY_POLL_HOUR_24 = int(_get_env("DISCOVERY_POLL_HOUR_24", "12"))
DISCOVERY_POLL_MINUTE = int(_get_env("DISCOVERY_POLL_MINUTE", "5"))

RUN_JOB_CONCURRENCY = int(_get_env("RUN_JOB_CONCURRENCY", "20"))
CHECKPOINT_SCRAPE_CHUNK_SIZE = int(_get_env("CHECKPOINT_SCRAPE_CHUNK_SIZE", "10"))
CHECKPOINT_JOB_CLAIM_LIMIT = int(_get_env("CHECKPOINT_JOB_CLAIM_LIMIT", "20"))
STALE_JOB_MINUTES = int(_get_env("STALE_JOB_MINUTES", "10"))
SUPABASE_URL = _get_env("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = _get_env("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_MEDIA_BUCKET = _get_env("SUPABASE_MEDIA_BUCKET", "fire-media")
MEDIA_STORAGE_PROVIDER = (_get_env("MEDIA_STORAGE_PROVIDER", "supabase").strip().lower() or "supabase")
R2_ENDPOINT_URL = _get_env("R2_ENDPOINT_URL")
R2_ACCESS_KEY_ID = _get_env("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = _get_env("R2_SECRET_ACCESS_KEY")
R2_BUCKET = _get_env("R2_BUCKET", SUPABASE_MEDIA_BUCKET)
R2_REGION = _get_env("R2_REGION", "auto")
MEDIA_PUBLIC_BASE_URL = _get_env("MEDIA_PUBLIC_BASE_URL")
R2_MEDIA_ENABLED = bool(
    MEDIA_STORAGE_PROVIDER == "r2"
    and R2_ENDPOINT_URL
    and R2_ACCESS_KEY_ID
    and R2_SECRET_ACCESS_KEY
    and R2_BUCKET
)
SUPABASE_MEDIA_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_MEDIA_BUCKET)
FIRE_MEDIA_RETENTION_ENABLED = R2_MEDIA_ENABLED or SUPABASE_MEDIA_ENABLED
WEB_PUSH_VAPID_PUBLIC_KEY = _get_env("WEB_PUSH_VAPID_PUBLIC_KEY")
WEB_PUSH_VAPID_PRIVATE_KEY = _get_env("WEB_PUSH_VAPID_PRIVATE_KEY")
WEB_PUSH_SUBJECT = _get_env("WEB_PUSH_SUBJECT", "mailto:alerts@feedme.local")

# --- Feeder intelligence (reels-only fingerprints + feeder file compilation) ---
FEEDER_INTELLIGENCE_PROVIDER = (
    _get_env("FEEDER_INTELLIGENCE_PROVIDER", _get_env("POST_INTELLIGENCE_PROVIDER", "auto")).strip().lower()
    or "auto"
)
OPENROUTER_API_KEY = _get_env("OPENROUTER_API_KEY")
OPENROUTER_BASE_URL = _get_env("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")
GEMINI_API_KEY = _get_env("GEMINI_API_KEY")
FEEDER_INTELLIGENCE_ENABLED = bool(OPENROUTER_API_KEY or GEMINI_API_KEY)
FEEDER_FINGERPRINT_MODEL = _get_env("FEEDER_FINGERPRINT_MODEL", _get_env("POST_INTELLIGENCE_MODEL"))
POST_CONDENSATION_MODEL = _get_env("POST_CONDENSATION_MODEL", "anthropic/claude-haiku-4.5")
D7_READ_MODEL = _get_env("D7_READ_MODEL", "anthropic/claude-haiku-4.5")
FEEDER_INTELLIGENCE_AUTO_LIMIT = int(_get_env("FEEDER_INTELLIGENCE_AUTO_LIMIT", "10"))
FEEDER_INTELLIGENCE_AUTO_INTERVAL_SECONDS = int(_get_env("FEEDER_INTELLIGENCE_AUTO_INTERVAL_SECONDS", "300"))
