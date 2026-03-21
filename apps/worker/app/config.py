import os


def _get_env(name: str, default: str | None = None, required: bool = False) -> str:
    value = os.getenv(name, default)
    if required and not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value or ""


POSTGRES_DSN = _get_env("POSTGRES_DSN", required=True)
APIFY_TOKEN = _get_env("APIFY_TOKEN", required=True)
APIFY_ACTOR_ID = _get_env("APIFY_ACTOR_ID", "apify/instagram-scraper")
APIFY_RUN_TIMEOUT_SECONDS = int(_get_env("APIFY_RUN_TIMEOUT_SECONDS", "900"))
APIFY_POLL_INTERVAL_SECONDS = int(_get_env("APIFY_POLL_INTERVAL_SECONDS", "10"))
RETRY_BACKOFF_MINUTES = [
    int(x.strip()) for x in _get_env("RETRY_BACKOFF_MINUTES", "15,15,15,30,30,60").split(",") if x.strip()
] or [15]
APP_TIMEZONE = _get_env("APP_TIMEZONE", "Asia/Kolkata")
CHECKPOINT_BATCH_HOUR_24 = int(_get_env("CHECKPOINT_BATCH_HOUR_24", "23"))
CHECKPOINT_BATCH_MINUTE = int(_get_env("CHECKPOINT_BATCH_MINUTE", "30"))

RUN_JOB_CONCURRENCY = int(_get_env("RUN_JOB_CONCURRENCY", "20"))
SUPABASE_URL = _get_env("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = _get_env("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_MEDIA_BUCKET = _get_env("SUPABASE_MEDIA_BUCKET", "fire-media")
FIRE_MEDIA_RETENTION_ENABLED = bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY and SUPABASE_MEDIA_BUCKET)
WEB_PUSH_VAPID_PUBLIC_KEY = _get_env("WEB_PUSH_VAPID_PUBLIC_KEY")
WEB_PUSH_VAPID_PRIVATE_KEY = _get_env("WEB_PUSH_VAPID_PRIVATE_KEY")
WEB_PUSH_SUBJECT = _get_env("WEB_PUSH_SUBJECT", "mailto:alerts@feedme.local")
