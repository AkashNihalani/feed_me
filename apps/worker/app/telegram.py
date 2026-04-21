"""Telegram alerting for Feed Me engine.

Sends notifications when:
  - Jobs fail permanently (exhausted all resurrections)
  - Worker starts/restarts
  - Daily summary (optional)

Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.
If not set, alerting is silently disabled.
"""

from __future__ import annotations

import html
import os
import traceback
from datetime import datetime, timezone

import requests


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

_ENABLED = bool(BOT_TOKEN and CHAT_ID)


def _send(text: str) -> bool:
    """Send a Telegram message. Returns True on success."""
    if not _ENABLED:
        return False
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        resp = requests.post(url, json={
            "chat_id": CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=10)
        return resp.ok
    except Exception:
        return False


def alert_worker_started() -> None:
    """Send alert when worker boots up."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    _send(f"🟢 <b>FeedMe Worker Started</b>\n{now}")


def alert_worker_error(error: Exception) -> None:
    """Send alert when worker hits an unexpected error."""
    tb = traceback.format_exception_only(type(error), error)
    msg = "".join(tb)[:500]
    _send(f"🔴 <b>FeedMe Worker Error</b>\n<pre>{msg}</pre>")


def alert_job_failed(job_type: str, job_id: int, feeder_handle: str, attempt: int, last_error: str) -> None:
    """Instant alert when any job fails (retry or final)."""
    _send(
        f"⚠️ <b>Job Failed</b>\n"
        f"@{feeder_handle} — {job_type}\n"
        f"Attempt #{attempt} | Job #{job_id}\n"
        f"<pre>{last_error[:300]}</pre>"
    )


def alert_job_skipped(job_type: str, job_id: int, feeder_handle: str, reason: str) -> None:
    """Alert when a hard failure is terminally skipped (no retries)."""
    _send(
        f"⏭️ <b>Job Skipped (Hard Failure)</b>\n"
        f"@{feeder_handle} — {job_type}\n"
        f"Job #{job_id}\n"
        f"<pre>{(reason or 'hard failure')[:300]}</pre>"
    )


def alert_permanently_failed(job_type: str, job_id: int, feeder_handle: str, last_error: str) -> None:
    """Send alert when a job exhausts all retries + resurrections."""
    _send(
        f"💀 <b>Job Dead — Giving Up</b>\n"
        f"@{feeder_handle} — {job_type}\n"
        f"Job #{job_id}\n"
        f"<pre>{last_error[:300]}</pre>"
    )


def alert_daily_summary(
    run_ok: int, run_fail: int,
    checkpoint_ok: int, checkpoint_fail: int,
    pending_run: int, pending_checkpoint: int,
) -> None:
    """Send daily engine health summary at 8 AM IST."""
    total_fail = run_fail + checkpoint_fail
    if total_fail == 0:
        _send(
            f"☀️ <b>Good Morning — All Smooth</b>\n"
            f"Runs: ✅ {run_ok} done | ⏳ {pending_run} queued\n"
            f"Checkpoints: ✅ {checkpoint_ok} done | ⏳ {pending_checkpoint} queued"
        )
    else:
        _send(
            f"📊 <b>Daily Report — {total_fail} Failed</b>\n"
            f"Runs: ✅ {run_ok} | ❌ {run_fail} | ⏳ {pending_run}\n"
            f"Checkpoints: ✅ {checkpoint_ok} | ❌ {checkpoint_fail} | ⏳ {pending_checkpoint}"
        )


def alert_post_intelligence_source_issue(
    *,
    handle: str | None,
    post_key: str,
    media_type: str,
    reason: str,
    expected_source: str | None = None,
    actual_source: str | None = None,
    detail: str | None = None,
    post_url: str | None = None,
) -> None:
    """Alert when post intelligence cannot analyze a post from the required source."""
    safe_handle = html.escape(f"@{handle}" if handle else "@unknown")
    safe_post_key = html.escape(post_key)
    safe_media_type = html.escape(media_type)
    safe_reason = html.escape(reason)
    lines = [
        "🚨 <b>Post Intelligence Source Mismatch</b>",
        f"{safe_handle} — {safe_media_type}",
        f"Post: <code>{safe_post_key}</code>",
        f"Reason: <code>{safe_reason}</code>",
    ]
    if expected_source:
        lines.append(f"Expected: <code>{html.escape(expected_source)}</code>")
    if actual_source:
        lines.append(f"Actual: <code>{html.escape(actual_source)}</code>")
    if detail:
        lines.append(f"Detail: <code>{html.escape(detail[:300])}</code>")
    if post_url:
        lines.append(f"<a href=\"{html.escape(post_url, quote=True)}\">Open Instagram post</a>")
    _send("\n".join(lines))


def is_enabled() -> bool:
    return _ENABLED
