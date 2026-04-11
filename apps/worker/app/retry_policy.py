from __future__ import annotations

from datetime import datetime, timedelta, timezone

import psycopg

from .config import RETRY_BACKOFF_MINUTES

_HARD_SKIP_PREFIX = 'hard-skip:'
_TRANSIENT_FAILURE_TOKENS = (
    'timeout',
    'timed out',
    'too many requests',
    '429',
    'rate limit',
    'connection',
    'network',
    'temporarily unavailable',
    'service unavailable',
    'bad gateway',
    'gateway timeout',
    'internal server error',
    'connection reset',
    'bright data json parse error',
    'json decode',
    'extra data',
    'expecting value',
    'customer is not active',
    'account is not active',
    'trial expired',
    'subscription',
    'billing',
)
_HARD_FAILURE_TOKENS = (
    'not found',
    'private',
    'deleted',
    'post missing',
    'missing in checkpoint batch',
    'missing post',
)
_PUSH_TRANSIENT_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
_DB_CONNECTION_ERROR_TOKENS = (
    "connection is closed",
    "server closed the connection unexpectedly",
    "consuming input failed",
    "ssl error",
    "unexpected eof",
    "broken pipe",
    "connection reset",
    "connection refused",
    "terminating connection",
    "could not receive data from server",
)


def next_retry_time(attempt: int) -> datetime:
    idx = min(attempt - 1, len(RETRY_BACKOFF_MINUTES) - 1)
    return datetime.now(timezone.utc) + timedelta(minutes=RETRY_BACKOFF_MINUTES[idx])


def normalize_error(err: str | None) -> str:
    return (err or '').strip().lower()


def is_transient_failure(err: str | None) -> bool:
    e = normalize_error(err)
    return any(tok in e for tok in _TRANSIENT_FAILURE_TOKENS)


def is_hard_failure(err: str | None) -> bool:
    e = normalize_error(err)
    if any(tok in e for tok in _HARD_FAILURE_TOKENS):
        return True
    return not is_transient_failure(e)


def hard_skip_error(err: str | None, fallback: str) -> str:
    msg = (err or '').strip() or fallback
    return f"{_HARD_SKIP_PREFIX}{msg[:900]}"


def should_retry_web_push(status_code: int | None, err: str | None) -> bool:
    if status_code in _PUSH_TRANSIENT_STATUS_CODES:
        return True
    return is_transient_failure(err)


def is_connection_error(exc: Exception | None) -> bool:
    if exc is None:
        return False
    msg = normalize_error(str(exc))
    if any(tok in msg for tok in _DB_CONNECTION_ERROR_TOKENS):
        return True
    return isinstance(exc, psycopg.OperationalError)
